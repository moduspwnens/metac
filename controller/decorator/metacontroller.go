/*
Copyright 2018 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package decorator

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/pkg/errors"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"openebs.io/metac/apis/metacontroller/v1alpha1"
	mcinformers "openebs.io/metac/client/generated/informers/externalversions"
	mclisters "openebs.io/metac/client/generated/listers/metacontroller/v1alpha1"
	"openebs.io/metac/config"
	"openebs.io/metac/controller/common"
	dynamicclientset "openebs.io/metac/dynamic/clientset"
	dynamicdiscovery "openebs.io/metac/dynamic/discovery"
	dynamicinformer "openebs.io/metac/dynamic/informer"
	k8s "openebs.io/metac/third_party/kubernetes"
)

type Metacontroller struct {
	resourceManager *dynamicdiscovery.APIResourceManager
	clientset       *dynamicclientset.Clientset
	informerFactory *dynamicinformer.SharedInformerFactory

	lister   mclisters.DecoratorControllerLister
	informer cache.SharedIndexInformer

	workerCount          int
	queue                workqueue.RateLimitingInterface
	decoratorControllers map[string]*decoratorController

	crdWatchingDisabled         bool
	configPathControllerConfigs []*v1alpha1.DecoratorController

	stopCh, doneCh chan struct{}

	// Total timeout for any condition to succeed.
	//
	// NOTE:
	//	This is currently used to load config that is required
	// to run Metac.
	waitTimeoutForCondition time.Duration

	// Interval between retries for any condition to succeed.
	//
	// NOTE:
	// 	This is currently used to load config that is required
	// to run Metac
	waitIntervalForCondition time.Duration
}

// NewMetacontroller returns a new instance of Metacontroller
func NewMetacontroller(
	resourceMgr *dynamicdiscovery.APIResourceManager,
	clientset *dynamicclientset.Clientset,
	dynInformers *dynamicinformer.SharedInformerFactory,
	mcInformerFactory mcinformers.SharedInformerFactory,
	workerCount int,
	configPath string,
) *Metacontroller {

	mc := &Metacontroller{
		resourceManager: resourceMgr,
		clientset:       clientset,
		informerFactory: dynInformers,

		lister:   mcInformerFactory.Metacontroller().V1alpha1().DecoratorControllers().Lister(),
		informer: mcInformerFactory.Metacontroller().V1alpha1().DecoratorControllers().Informer(),

		queue:                workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "DecoratorController"),
		decoratorControllers: make(map[string]*decoratorController),
		workerCount:          workerCount,

		waitTimeoutForCondition:  30 * time.Minute,
		waitIntervalForCondition: 1 * time.Second,
	}

	var ctlsAsConfig []*v1alpha1.DecoratorController
	var ctlsAsConfigErr error
	// NOTE: ConfigPath has higher priority to get the
	// GenericController instances as configs to run Metac
	if configPath != "" {

		mc.crdWatchingDisabled = true

		mconfigs, err := config.New(configPath).Load()
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("failed to load DecoratorController configs from config path: %v", configPath))
			return nil
		}
		ctlsAsConfig, ctlsAsConfigErr = mconfigs.ListDecoratorControllers()
	} else {
		mc.crdWatchingDisabled = false
	}

	if ctlsAsConfigErr != nil {
		utilruntime.HandleError(fmt.Errorf("failed to load DecoratorControllers from config path: %v", configPath))
		return nil
	}

	mc.configPathControllerConfigs = ctlsAsConfig

	mc.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    mc.enqueueDecoratorController,
		UpdateFunc: mc.updateDecoratorController,
		DeleteFunc: mc.enqueueDecoratorController,
	})

	return mc
}

func (mc *Metacontroller) String() string {
	return "Local DecoratorController"
}

// Start this controller
func (mc *Metacontroller) Start() {
	mc.stopCh = make(chan struct{})
	mc.doneCh = make(chan struct{})

	go func() {
		defer close(mc.doneCh)
		defer utilruntime.HandleCrash()

		glog.Info("Starting DecoratorController metacontroller")

		if mc.crdWatchingDisabled {

			glog.Infof("DecoratorController ConfigPath controller configs: %v", mc.configPathControllerConfigs)

			// we run this as a continuous process
			// until all the configs are loaded
			condErr := mc.wait(mc.startAllDecoratorControllers)
			if condErr != nil {
				glog.Fatalf("%s: Failed to start: %v", mc, condErr)
			}

		} else {
			defer glog.Info("Shutting down DecoratorController metacontroller")

			if !k8s.WaitForCacheSync("DecoratorController", mc.stopCh, mc.informer.HasSynced) {
				return
			}

			// In the metacontroller, we are only responsible for starting/stopping
			// the actual controllers, so a single worker should be enough.
			for mc.processNextWorkItem() {
			}
		}
	}()
}

// wait polls the condition until it's true, with a configured
// interval and timeout.
//
// The condition function returns a bool indicating whether it
// is satisfied, as well as an error which should be non-nil if
// and only if the function was unable to determine whether or
// not the condition is satisfied (for example if the check
// involves calling a remote server and the request failed).
func (mc *Metacontroller) wait(condition func() (bool, error)) error {
	// mark the start time
	start := time.Now()
	for {
		done, err := condition()
		if err == nil && done {
			return nil
		}
		if time.Since(start) > mc.waitTimeoutForCondition {
			return errors.Errorf(
				"%s: Wait condition timed out %s: %v", mc, mc.waitTimeoutForCondition, err,
			)
		}
		if err != nil {
			// Log error, but keep trying until timeout.
			glog.V(4).Infof("%s: Wait condition failed: Will retry: %v", mc, err)
		} else {
			glog.V(4).Infof("%s: Waiting for condition to succeed: Will retry", mc)
		}
		time.Sleep(mc.waitIntervalForCondition)
	}
}

func (mc *Metacontroller) startAllDecoratorControllers() (bool, error) {
	for _, c := range mc.configPathControllerConfigs {
		err := mc.syncDecoratorController(c)

		if err != nil {
			glog.V(4).Infof("Error occurred starting decorator controller %v: %v", c.ObjectMeta.Name, err)
			return false, err
		}
	}
	return true, nil
}

// Stop this controller
func (mc *Metacontroller) Stop() {
	// Stop metacontroller first so there's no more changes to controllers.
	close(mc.stopCh)
	mc.queue.ShutDown()
	<-mc.doneCh

	// Stop all controllers.
	var wg sync.WaitGroup
	for _, c := range mc.decoratorControllers {
		wg.Add(1)
		go func(c *decoratorController) {
			defer wg.Done()
			c.Stop()
		}(c)
	}
	wg.Wait()
}

func (mc *Metacontroller) processNextWorkItem() bool {
	key, quit := mc.queue.Get()
	if quit {
		return false
	}
	defer mc.queue.Done(key)

	err := mc.sync(key.(string))
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to sync DecoratorController %q: %v", key, err))
		mc.queue.AddRateLimited(key)
		return true
	}

	mc.queue.Forget(key)
	return true
}

func (mc *Metacontroller) sync(key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	glog.V(4).Infof("sync DecoratorController %v", name)

	dc, err := mc.lister.Get(name)
	if apierrors.IsNotFound(err) {
		glog.V(4).Infof("DecoratorController %v has been deleted", name)
		// Stop and remove the controller if it exists.
		if c, ok := mc.decoratorControllers[name]; ok {
			c.Stop()
			delete(mc.decoratorControllers, name)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return mc.syncDecoratorController(dc)
}

func (mc *Metacontroller) syncDecoratorController(dc *v1alpha1.DecoratorController) error {
	if c, ok := mc.decoratorControllers[dc.Name]; ok {
		// The controller was already started.
		if apiequality.Semantic.DeepEqual(dc.Spec, c.schema.Spec) {
			// Nothing has changed.
			return nil
		}
		// Stop and remove the controller so it can be recreated.
		c.Stop()
		delete(mc.decoratorControllers, dc.Name)
	}

	c, err := newDecoratorController(mc.resourceManager, mc.clientset, mc.informerFactory, dc)
	if err != nil {
		return err
	}
	c.Start(mc.workerCount)
	mc.decoratorControllers[dc.Name] = c
	return nil
}

func (mc *Metacontroller) enqueueDecoratorController(obj interface{}) {
	key, err := common.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}
	mc.queue.Add(key)
}

func (mc *Metacontroller) updateDecoratorController(old, cur interface{}) {
	mc.enqueueDecoratorController(cur)
}
