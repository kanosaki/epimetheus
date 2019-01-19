import axios from 'axios'

export async function jobStatus() {
  const resp = await axios.get(`http://localhost:9090/epi/v1/job/status`)
  return new JobStatus(resp.data)
}

export class JobStatus {
  constructor(data) {
    this.data = data
  }
}


export async function jobDiscovery() {
  const resp = await axios.get(`http://localhost:9090/epi/v1/job/discovery`)
  return new JobDiscovery(resp.data)
}

export class JobDiscovery {
  constructor(data) {
    this.data = data
  }
  name() {
    return this.data.config.job_name;
  }
}

