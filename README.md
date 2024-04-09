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


## add samples
```shell
datahub docker ingest-sample-data
```

## nuke
```shell
datahub docker nuke
```

## add custom platform image
```shell
datahub put platform --name exasol --display_name "Exasol" --logo "https://www.exasol.com/app/uploads/2020/06/favicon-260x260-1-150x150.png"
```
