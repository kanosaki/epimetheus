import axios from "axios";

export async function jobStatus() {
  const resp = await axios.get(`http://localhost:9090/epi/v1/job/status`);
  return new JobStatus(resp.data);
}

export class JobStatus {
  private data: any;

  constructor(data) {
    this.data = data;
  }
}

export async function jobDiscovery() {
  const resp = await axios.get(`http://localhost:9090/epi/v1/job/discovery`);
  return new JobDiscovery(resp.data);
}

export async function createJobDiscovery(data: ScrapeDiscovery) {
  await axios.post(`http://localhost:9090/epi/v1/job/discovery`, data)
}

interface ScrapeDiscovery {
  config: ScrapeConfig;
  refreshInterval: number;
  lastRefresh: number;
}

// Refer Prometheus configuration format
interface ScrapeConfig {
  job_name: string;
  scrape_interval: number;
  metrics_path: string;
  honor_labels: string;
  scheme: string;
  params: Map<string, Array<string>>;
  static_configs: Array<StaticConfig>;
}

interface StaticConfig {
  targets: Array<string>;
  labels: Map<string, string>;
}

export class JobDiscovery {
  private data: ScrapeDiscovery;

  constructor(data) {
    this.data = data;
  }

  name() {
    return this.data.config.job_name;
  }
}
