* **Run gRPC server:**

|./grpc_server|
|:---|


* **Run gRPC client:**

|./grpc_client -n=10000000 -c=100 -peak_size=110|
|:---|

&nbsp;&nbsp;&nbsp;&nbsp;**Or:**

|./grpc_client -server=127.0.0.1:2020 -n=10000000 -c=100 -peak_size=110|
|:---|
