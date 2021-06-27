## http-sidecar:   
 


### start http-sidecar locally and verify:

```
cd ~/workspace
git clone git@github.com:networknt/light-mesh.git

cd light-mesh

mvn clean install

cd http-sidecar

java -jar -Dlight-4j-config-dir=configuration/local-config  target/http-sidecar.jar


```

