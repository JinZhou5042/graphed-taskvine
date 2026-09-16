# Security

## Reporting

Report security-sensitive issues privately to the repository owner before opening a public issue.
Include the affected version, deployment assumptions, and a minimal reproduction that does not
contain credentials or private data.

## Trust model

This package transports executable Python callables with `cloudpickle`. A serialized plan, shipped
module, worker result, or restored cache entry can execute code in the receiving process. They must
all come from trusted project members and infrastructure.

The adapter does not provide:

- authentication or authorization for TaskVine workers;
- isolation from malicious Python callables;
- validation that a partition URI is safe to read;
- secret management;
- exactly-once execution of task side effects.

Configure network access and worker identity through TaskVine and the deployment environment. Do
not embed credentials in plans, partition URIs, logs, or shipped source files. Use external secret
delivery appropriate to the cluster.

TaskVine may retry work after a worker failure. Functions that write outside their task sandbox
must use idempotent operations or content-addressed destinations.
