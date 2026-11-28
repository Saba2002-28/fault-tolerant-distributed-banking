apiVersion: v1
kind: Service
metadata:
  name: shard-a-db
  namespace: banking-assignment3
spec:
  clusterIP: None
  selector:
    app: shard-a-db
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: shard-a-db
  namespace: banking-assignment3
spec:
  serviceName: shard-a-db
  replicas: 1
  selector:
    matchLabels:
      app: shard-a-db
  template:
    metadata:
      labels:
        app: shard-a-db
    spec:
      containers:
        - name: postgres
          image: postgres:17
          args:
            - postgres
            - -c
            - max_prepared_transactions=100
          ports:
            - containerPort: 5432
              name: postgres
          env:
            - name: POSTGRES_DB
              value: shard_a
            - name: POSTGRES_USER
              value: bankuser
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: banking-db-secret
                  key: POSTGRES_PASSWORD
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
          readinessProbe:
            exec:
              command:
                - sh
                - -c
                - pg_isready -U bankuser -d shard_a
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: shard-b-db
  namespace: banking-assignment3
spec:
  clusterIP: None
  selector:
    app: shard-b-db
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: shard-b-db
  namespace: banking-assignment3
spec:
  serviceName: shard-b-db
  replicas: 1
  selector:
    matchLabels:
      app: shard-b-db
  template:
    metadata:
      labels:
        app: shard-b-db
    spec:
      containers:
        - name: postgres
          image: postgres:17
          args:
            - postgres
            - -c
            - max_prepared_transactions=100
          ports:
            - containerPort: 5432
              name: postgres
          env:
            - name: POSTGRES_DB
              value: shard_b
            - name: POSTGRES_USER
              value: bankuser
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: banking-db-secret
                  key: POSTGRES_PASSWORD
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
          readinessProbe:
            exec:
              command:
                - sh
                - -c
                - pg_isready -U bankuser -d shard_b
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: coordinator-db
  namespace: banking-assignment3
spec:
  clusterIP: None
  selector:
    app: coordinator-db
  ports:
    - name: postgres
      port: 5432
      targetPort: 5432
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: coordinator-db
  namespace: banking-assignment3
spec:
  serviceName: coordinator-db
  replicas: 1
  selector:
    matchLabels:
      app: coordinator-db
  template:
    metadata:
      labels:
        app: coordinator-db
    spec:
      containers:
        - name: postgres
          image: postgres:17
          ports:
            - containerPort: 5432
              name: postgres
          env:
            - name: POSTGRES_DB
              value: coordinator
            - name: POSTGRES_USER
              value: bankuser
            - name: POSTGRES_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: banking-db-secret
                  key: POSTGRES_PASSWORD
          resources:
            requests:
              cpu: 100m
              memory: 256Mi
            limits:
              cpu: 500m
              memory: 512Mi
          readinessProbe:
            exec:
              command:
                - sh
                - -c
                - pg_isready -U bankuser -d coordinator
            initialDelaySeconds: 5
            periodSeconds: 5
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 1Gi
