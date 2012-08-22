// Release version of debug helpers

// +build release

package ybc

import (
	"time"
)

/*******************************************************************************
 * debugGuard
 ******************************************************************************/

type debugGuard struct{}

func (dg *debugGuard) Init() {}

func (dg *debugGuard) InitByOwner() {}

func (db *debugGuard) InitNoClose() {}

func (dg *debugGuard) CheckLive() {}

func (dg *debugGuard) Close() {}

func (dg *debugGuard) CloseByOwner() {}

/*******************************************************************************
 * cacheGuard
 ******************************************************************************/

type cacheGuard struct{}

func (cg *cacheGuard) SetDataFile(dataFile string) {}

func (cg *cacheGuard) SetIndexFile(indexFile string) {}

func (cg *cacheGuard) Acquire() {}

func (cg *cacheGuard) Release() {}

/*******************************************************************************
 * clusterCacheGuard
 ******************************************************************************/

type clusterCacheGuard struct{}

func debugAcquireClusterCache(configs []*Config) (ccg clusterCacheGuard) { return }

func debugReleaseClusterCache(ccg clusterCacheGuard) {}

/*******************************************************************************
 * misc functions
 ******************************************************************************/

func checkNonNegative(n int) {}

func checkNonNegativeDuration(t time.Duration) {}
