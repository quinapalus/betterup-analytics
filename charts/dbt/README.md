# DBT Helm Chart

This helm chart is for running the DBT project at 
[betterup-analytics/warehouse](https://github.com/betterup/betterup-analytics/tree/main/warehouse).

# Install for `dbt-gov-dev` using Secrets

Install a `dbt run` CronJob in the gov dev cluster.

```bash
helm install dbt-gov-dev . -f secrets://secrets/gov-dev.values.yaml -n dbt
```

This will install a `dbt-gov-dev` CronJob on the cluster.

```
NAME: dbt-gov-dev
LAST DEPLOYED: [...]
NAMESPACE: dbt
STATUS: deployed
REVISION: 1
TEST SUITE: None
NOTES:
See your DBT cronjob here by running:

$ kubectl get cronjob -n dbt

Start a `dbt run` job manually with

$ kubectl create job --from cronjobs/dbt-gov-dev dbt-run -n dbt

The next DBT job will be created at the top of the hour.
```

It will also generate some useful commands for you in the `NOTES`!

# Install Each DBT `CronJob` Manually

Install main `dbt build` cronjob. Set the $DEPLOY_ENV to `dev`, `stage` or `prod`.

```bash
helm install dbt-build . -n dbt -f cronjobs/build.values.yaml  -f secrets://secrets/gov-$DEPLOY_ENV.values.yaml
helm install dbt-anon . -n dbt -f cronjobs/anon.values.yaml  -f secrets://secrets/gov-$DEPLOY_ENV-anon.values.yaml
```
