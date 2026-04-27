### Please note

#### Prerequisite
Before creating CRD `Kafka` in `kafka.yaml` execute below step

```bash
kubectl create secret generic local--kafka --from-file=plain_credentials.json
```

#### Applying Updated Plain Secret
Suppose u did some update in `plain_credentials.json`, to propagate the updated password, you have to delete and create secret
and then do a rolling restart of kafka brokers by annotating `strimzipodset`

```bash
kubectl delete secret local--kafka
kubectl create secret generic local--kafka --from-file=plain_credentials.json
```

Apply rolling restart annotation.

```bash
kubectl annotate strimzipodset my-cluster-kafka strimzi.io/manual-rolling-update="true" --overwrite
```

Once rolling restart is done remove the annotation

```bash
kubectl annotate strimzipodset my-cluster-kafka strimzi.io/manual-rolling-update-
```