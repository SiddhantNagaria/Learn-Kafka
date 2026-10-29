# Kafka on Ubuntu + Kafbat UI + Conduktor

Minimal setup guide for running Kafka in Ubuntu/WSL, testing it from Windows PowerShell, and viewing it in Kafbat UI and Conduktor.

## 1. One-time setup: persistent storage + format

Kafka 4.x runs in KRaft mode only. The storage directory must be **formatted once** before the first start, otherwise Kafka exits with `No readable meta.properties files found`.

The default `log.dirs` is `/tmp/kraft-combined-logs`, and `/tmp` is wiped when WSL restarts. Move the data to a permanent folder **before** formatting:

```bash
cd ~/kafka_2.13-4.2.2
mkdir -p ~/kafka-data
sed -i 's|^log.dirs=.*|log.dirs=/home/siddhant/kafka-data|' config/server.properties
grep log.dirs config/server.properties
```

The `grep` should print `log.dirs=/home/siddhant/kafka-data`. Use a Linux path, not `/mnt/c/...`.

Format the storage (run once):

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
```

Notes:
- `--standalone` is for a single-node setup. If your version rejects it, run the same command without it.
- If it says the directory is already formatted, skip this step and just start Kafka.
- Formatting creates an empty cluster. Re-formatting an existing data directory wipes its topics and consumer group offsets.

## 2. Configure Kafka in Ubuntu

Get the current Ubuntu/WSL IP:

```bash
hostname -I
```

Back up the config once:

```bash
cp config/server.properties config/server.properties.bak
```

Edit the config:

```bash
nano config/server.properties
```

For a single-node KRaft broker/controller, ensure these settings are present (keep the existing `process.roles`, `node.id`, and quorum settings unchanged). Have only one active `listeners` and one active `advertised.listeners` line:

```properties
listeners=PLAINTEXT://0.0.0.0:9092,CONTROLLER://localhost:9093
advertised.listeners=PLAINTEXT://YOUR_UBUNTU_IP:9092
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
log.dirs=/home/siddhant/kafka-data
```

Replace `YOUR_UBUNTU_IP` with the IP from `hostname -I`. Do not include the controller listener in `advertised.listeners`.

Save in nano: `Ctrl+O`, Enter, `Ctrl+X`.

## 3. Start / stop Kafka

Start the broker in Ubuntu and keep this terminal open:

```bash
cd ~/kafka_2.13-4.2.2
bin/kafka-server-start.sh config/server.properties
```

Stop it with `Ctrl+C`.

After the one-time format in step 1, this is the only command you need on later days. Do not format again unless the data directory was lost.

## 4. Test Kafka in Ubuntu

Open a second Ubuntu terminal:

```bash
cd ~/kafka_2.13-4.2.2
bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Create and verify a test topic:

```bash
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic first_topic
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

For a single broker, use replication factor `1`. Replication factor `2` requires at least two brokers.

## 5. Test the connection from Windows PowerShell

Run in **Windows PowerShell**, not Ubuntu. Replace the IP with the current value from `hostname -I`:

```powershell
Test-NetConnection YOUR_UBUNTU_IP -Port 9092
```

Expected: `TcpTestSucceeded : True`.

If it is `False`, check that Kafka is running, the listener settings are correct, and Windows/WSL networking or firewall rules allow the connection. WSL IPs can change after restart, so recheck `hostname -I` and update `advertised.listeners` when needed.

## 6. Kafbat UI in the browser

Open: http://localhost:8080 (or `http://localhost:9090` if you configured Kafbat to use port 9090).

When adding a cluster, enter:

- **Cluster name:** `Ubuntu Kafka`
- **Technical ID:** `ubuntu-kafka`
- **Bootstrap servers:** `YOUR_UBUNTU_IP:9092`

Use the same current Ubuntu IP that passed the Windows port test.

If using a YAML config instead of the web form, example `C:\kafka-ui\application-local.yml`:

```yaml
kafka:
  clusters:
    - name: Ubuntu Kafka
      bootstrapServers: YOUR_UBUNTU_IP:9092
```

Start the JAR with the config file (adjust the JAR path):

```powershell
java "-Dspring.config.additional-location=file:C:/kafka-ui/application-local.yml" -jar "C:\PATH\TO\kafka-ui-api.jar"
```

To run the UI on port 9090 instead of 8080:

```powershell
java "-Dserver.port=9090" "-Dspring.config.additional-location=file:C:/kafka-ui/application-local.yml" -jar "C:\PATH\TO\kafka-ui-api.jar"
```

## 7. Conduktor UI in the browser

Open Conduktor in the browser and add a cluster:

- **Cluster name:** `Ubuntu Kafka`
- **Technical ID:** `ubuntu-kafka`
- **Bootstrap servers:** `YOUR_UBUNTU_IP:9092`

Use the same Ubuntu IP that passed `Test-NetConnection`. If Conduktor reports `Host is unreachable` while Kafbat works, the Conduktor backend may be running in a different network context (for example, Docker or a remote/cloud environment). In that case, `localhost` and the Windows/WSL IP may not be reachable from that backend; configure a reachable broker address/network path for the actual Conduktor deployment.

If Conduktor runs in Docker on the same machine, also try `host.docker.internal:9092`. Make sure `advertised.listeners` is an address the container can reach.

## 8. Troubleshooting

**`No readable meta.properties files found`**
Storage is not formatted, or it was wiped (usually because `log.dirs` was in `/tmp` and WSL restarted). Do step 1: set `log.dirs` to a permanent path, then format and start.

**Consumer group reset fails: `Assignments can only be reset if the group is inactive, but the current state is Stable`**
A consumer is still running in that group. Stop all consumers (Ctrl+C in the `kafka-console-consumer.sh` terminal, or stop your app), wait 10-30 seconds, and check:

```bash
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-first-application --state
```

When it shows `Empty`, run the reset:

```bash
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --to-earliest --topic third_topic --dry-run
bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-first-application --reset-offsets --to-earliest --topic third_topic --execute
```

## Quick checks

Ubuntu broker listener:

```bash
ss -ltnp | grep 9092
```

Ubuntu cluster ID:

```bash
bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092
```

Data directory is formatted:

```bash
ls ~/kafka-data/meta.properties
```

Windows connectivity:

```powershell
Test-NetConnection YOUR_UBUNTU_IP -Port 9092
```

Remember: `YOUR_UBUNTU_IP` is a placeholder. Replace it with the current address from `hostname -I`.