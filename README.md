# DataHub

## Install
```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
datahub version
```

## Start & Stop
```shell
datahub docker quickstart
datahub docker quickstart --stop
```


## Backup & Restore
```shell
datahub docker quickstart --backup --backup-file ./backup.sql
datahub docker quickstart --restore --restore-file ./backup.sql
```
