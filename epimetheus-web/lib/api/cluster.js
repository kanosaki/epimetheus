import axios from 'axios'

export async function clusterInfo() {
  const resp = await axios.get(`http://localhost:9090/epi/v1/cluster`)
  return new ClusterInfo(resp.data)
}

export class ClusterInfo {
  constructor(data) {
    this.data = data
  }
}

export async function clusterStorage() {
  const resp = await axios.get(`http://localhost:9090/epi/v1/cluster/storage`)
  return new ClusterStorage(resp.data)
}

/* example data
 {
  "storageMetrics": null,
  "dataRegionMetrics": [
    {
      "name": "default",
      "totalAllocatedPages": 9231,
      "totalAllocatedSize": 38031720,
      "allocationRate": 153.85,
      "evictionRate": 0,
      "largeEntriesPagesPercentage": 0,
      "pagesFillFactor": 0.9999052,
      "dirtyPages": 0,
      "physicalMemoryPages": 9231,
      "physicalMemorySize": 38031720,
      "usedCheckpointBufferPages": 0,
      "usedCheckpointBufferSize": 0,
      "checkpointBufferSize": 0,
      "pageSize": 4096,
      "offHeapSize": 268435456,
      "pagesReplaceRate": 0,
      "pagesReplaceAge": 0,
      "pagesRead": 0,
      "pagesWritten": 0,
      "pagesReplaced": 0,
      "offheapUsedSize": 38031720
    }
  ]
}
 */
export class ClusterStorage {
  constructor(data) {
    this.data = data
  }
}
