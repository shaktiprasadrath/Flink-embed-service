# Docker and Kubernetes

The app runs Flink embedded, so the container is self-contained: one JVM, no Flink cluster, and no database server.

| File | Purpose |
|---|---|
| [`Dockerfile`](../Dockerfile) | Multi-stage build. The Gradle `installDist` step runs on JDK 21, and the result runs on the JRE 21 image (Ubuntu based) as a non-root user. |
| [`k8s/job.yaml`](../k8s/job.yaml) | **Job**. Runs the use cases once and prints the result tables to the pod log. |
| [`k8s/explorer.yaml`](../k8s/explorer.yaml) | **Deployment and Service**. Runs the use cases, shows the Flink Web UI during UC-21, then keeps the H2 console up so you can query results. |
| [`k8s/kustomization.yaml`](../k8s/kustomization.yaml) | Applies everything and sets the image name in one place. |

## Things to know first

1. **The app runs to completion.** It is a batch program, not a server. As a plain Deployment it would exit and be restarted over and over (CrashLoopBackOff). That is why the main manifest is a **Job**. The explorer Deployment only stays up because it sets `-Dflinkdemo.keepalive=true`.
2. **Results live in memory.** H2 is in-memory, so the tables exist only while the JVM runs. The Job prints them to its log; to query them, use the explorer.
3. **The H2 console has no login.** The explorer enables `-webAllowOthers` so the console can be reached through a port-forward. Keep the Service as `ClusterIP` and never expose it with NodePort, LoadBalancer or Ingress. This setup is for local testing only.
4. **The Flink Web UI runs only during UC-21.** It is up for `flinkdemo.webui.seconds`: 300 s in the explorer, 10 s in the Job. Inside a container it must bind to `0.0.0.0` (`-Dflinkdemo.webui.bind=0.0.0.0`).
5. **Resources.** Flink's MiniCluster and the 21 jobs need about 1–1.5 GB of heap. The manifests request 1 Gi / 1 CPU with a limit of 2 Gi / 2 CPU, and the JVM uses 75% of the container memory. Docker Desktop needs at least 4 GB assigned in Settings → Resources.
6. **CPU architecture.** The image matches the machine that built it (amd64 on a Windows PC). If anyone will pull it on Apple Silicon or ARM nodes, build a multi-arch image (see below). RocksDB (UC-16) ships native libraries for both, and needs glibc, so the image does not use Alpine.
7. **Line endings.** `gradlew` must have LF line endings inside Linux. `.gitattributes` enforces this, and the Dockerfile strips any CR characters as well.

## 1. Build the image

Start Docker Desktop first.

```bash
docker build -t flink-embed-service:0.1.0 .
```

The first build downloads Gradle and the dependencies (a few minutes). Rebuilds reuse the cached dependency layer. Tests are not run during the image build; run `./gradlew test` before building.

## 2. Run with Docker

Run everything and print the result tables:

```bash
docker run --rm flink-embed-service:0.1.0
```

Run selected use cases:

```bash
docker run --rm flink-embed-service:0.1.0 uc01 uc09 uc13
```

Run with the Web UI (during UC-21) and the H2 console. Stop it with Ctrl+C:

```bash
docker run --rm -it -p 8081:8081 -p 8082:8082 -e JAVA_OPTS="-XX:MaxRAMPercentage=75 -Dflinkdemo.data.dir=/app/data -Dflinkdemo.webui.seconds=300 -Dflinkdemo.webui.bind=0.0.0.0 -Dflinkdemo.keepalive=true -Dflinkdemo.h2.allowOthers=true" flink-embed-service:0.1.0
```

Then open:

- http://localhost:8081: Flink Web UI, while UC-21 runs
- http://localhost:8082: H2 console, after all use cases finish. Use JDBC URL `jdbc:h2:mem:flinkdemo`, user `sa`, and an empty password.

## 3. Push to Docker Hub

Replace `<user>` with your Docker Hub user name:

```bash
docker login
```

```bash
docker tag flink-embed-service:0.1.0 docker.io/<user>/flink-embed-service:0.1.0
```

```bash
docker push docker.io/<user>/flink-embed-service:0.1.0
```

Multi-arch alternative (amd64 + arm64). This builds and pushes in one step:

```bash
docker buildx build --platform linux/amd64,linux/arm64 -t docker.io/<user>/flink-embed-service:0.1.0 --push .
```

## 4. Deploy to Docker Desktop Kubernetes

Enable Kubernetes in Docker Desktop (Settings → Kubernetes) and select the context:

```bash
kubectl config use-context docker-desktop
```

**Which image to use:** Docker Desktop's Kubernetes can use the locally built `flink-embed-service:0.1.0` directly (`imagePullPolicy: IfNotPresent`), so pushing is optional. To use the Docker Hub image instead, set `newName: docker.io/<user>/flink-embed-service` in [`k8s/kustomization.yaml`](../k8s/kustomization.yaml).

Deploy:

```bash
kubectl apply -k k8s
```

### Test the Job

Watch it until `Complete` (about 3–4 minutes):

```bash
kubectl -n flink-demo get jobs -w
```

```bash
kubectl -n flink-demo logs job/flink-demo-run
```

The log ends with `All 21 use case(s) succeeded.`, and each use case's tables are printed above that line. Compare them with the expected values in [FEATURES.md](FEATURES.md).

A Job's pod template cannot be changed, so delete the Job before running it again:

```bash
kubectl -n flink-demo delete job flink-demo-run
```

```bash
kubectl apply -k k8s
```

### Test the explorer

```bash
kubectl -n flink-demo logs -f deploy/flink-demo-explorer
```

```bash
kubectl -n flink-demo port-forward deploy/flink-demo-explorer 8081:8081 8082:8082
```

- http://localhost:8081: Flink Web UI, while the log shows `Web UI running` (UC-21, 5 minutes)
- http://localhost:8082: H2 console, once the log shows `Keeping the JVM alive`. The pod is also `Ready` from that point on. Run, for example, `SELECT * FROM join_results`.

The port-forward targets the Deployment, not the Service. The Service only routes to Ready pods, and the pod becomes Ready only after UC-21 has finished.

### Clean up

```bash
kubectl delete -k k8s
```

## Running other use cases or options

Change `args` (for example `["uc03", "uc16"]`) or `JAVA_OPTS` in `k8s/job.yaml` / `k8s/explorer.yaml`. The supported `-Dflinkdemo.*` options are listed in the [README](../README.md#options), plus:

| Option | Effect |
|---|---|
| `-Dflinkdemo.keepalive=true` | After the run, keep the JVM and H2 console up until the process is stopped |
| `-Dflinkdemo.h2.allowOthers=true` | Let the H2 console accept non-localhost connections (needed in a container) |
| `-Dflinkdemo.webui.bind=0.0.0.0` | Bind the Flink Web UI to all interfaces (needed in a container) |
