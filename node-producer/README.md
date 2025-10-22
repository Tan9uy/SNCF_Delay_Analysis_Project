# Node Consumer

## Installer kafka en local
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install kafka bitnami/kafka
```

Forward port into kb8 :
```bash
kubectl -n default port-forward kafka-0 9092:9092
```

## Documentation
### Effects
- **REDUCED_SERVICE :** Arrêts enlevés
- **ADDITIONAL_SERVICE :** Train ajouté
- **NO_SERVICE :** Train annulé
- **SIGNIFICANT_DELAY :** Retard


## TODO

**/api/metrics/**
