## http-sidecar:   
 


### start http-sidecar locally and verify:

- start http-sidecar service

```
cd ~/workspace
git clone git@github.com:networknt/light-mesh.git

cd light-mesh

mvn clean install

cd http-sidecar

java -jar -Dlight-4j-config-dir=config/local  target/http-sidecar.jar


```

The http-sidecar service will start on http port 9080 and https port 9445. In the kubernetes multiple containers situation, the http-sidecar service will be deployed as sidecar container with the service API in same pod.
The service API could use any technologies, like NodeJs, .nets, php service...; The sidecar container (http-sidecar service) will handle the ingress and egress traffic to the pod and leverage light-4j cross-cutting concerns and client module features.

In the k8s pod, the egress network traffic will http protocol(from service API container to sidecar container), and the traffic will be forwarded by sidecar container (delegate light-router features) to downsteam API.

In reverse way, the ingress traffic (from outside of pod to call the service API in the pod) should be https protocol, it will reach the sidecar container first (by http-sidecar service Id), the sidecar container will leverage the light platform cross-cutting concerns features and forward the request to main service container in the pod.



- Start Nodejs restful API (It is simulate the service in the Pod) 

Follow the [steps](nodeapp/start.md) to start Nodejs books store restful API. The Nodejs api will start on local port: 8080 

We can verify the Nodejs restful API directly with curl command:

```
Get:

curl --location --request GET 'http://localhost:8080/api/books/' \
--header 'Content-Type: application/json' \
--data-raw '{"name":"mybook"}'

Post:

curl --location --request POST 'http://localhost:8080/api/books/' \
--header 'Content-Type: application/json' \
--data-raw '{"title":"Newbook"}'

Put:

curl --location --request POST 'http://localhost:8080/api/books/' \
--header 'Content-Type: application/json' \
--data-raw '{"title":"Newbook"}'

Delete:

curl --location --request DELETE 'http://localhost:8080/api/books/4' \
--header 'Content-Type: application/json' \
```


- Start a sample light-4j API from light-example-4j (It is simulate the outside service which service in the Pod need to call):


```
 cd ~/networknt
 git clone git@github.com:networknt/light-example-4j.git
 cd ~/networknt/light-example-4j/servicemesher/services

 mvn clean install -Prelease

cd petstore-service-api

java -jar target/petstore-service-api-3.0.1.jar

```

The petstore light-api will start on local https 8443 port. 


- Try the call by using http-sidecar:

#### Egress traffic (http protocol, port 9080)

Send request from service in the pod to light API petstore through sidecar

```
curl --location --request GET 'http://localhost:9080/v1/pets' \
--header 'Content-Type: application/json' \
--data-raw '{"accountId":1,"transactioType":"DEPOSIT","amount":20}'
```

#### Ingress traffic (https protocol, port 9445)

Send request from outside service to the service in the pod through sidecar

```
curl --location --request GET 'https://localhost:9445/api/books/' \
--header 'Content-Type: application/json' \
--data-raw '{"name":"mybook"}'
```

Leverage schema validation handler cross-cutting concerns

```
curl --location --request POST 'https://localhost:9445/api/books/' \
--header 'Content-Type: application/json' \
--data-raw '{"author":"Steve Jobs"}'
```

response:

```
{
    "statusCode": 400,
    "code": "ERR11004",
    "message": "VALIDATOR_SCHEMA",
    "description": "Schema Validation Error - requestBody.title: is missing but it is required",
    "severity": "ERROR"
}
```


