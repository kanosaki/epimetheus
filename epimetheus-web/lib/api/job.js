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
