---
title: Running Pirk in Cloud Environments (GCP, AWS, Azure)
nav: nav_commercial_cloud
---

## Google Cloud Platform (GCP)
This guide is a walkthrough with steps to get Pirk running on Google's Cloud Platform. 


### Steps
 1. Create a google cloud account. You may be eligible for a first-60-days-or-$300 credit. You can do this from [https://cloud.google.com/](https://cloud.google.com/).
 2. Install the [`gcloud` command line tool](https://cloud.google.com/sdk/) and run `gcloud init`. This will let you authorize the `gcloud` tool against your Google account.
 3. Create a new project. For example, `pirkongcpexample`
 4. Enable billing for that project (somewhere in the user interface). If you are a free trial user you may not need to change anything in the billing settings. 
 5. Enable the dataproc API. At the time of writing, [this page API](https://console.developers.google.com/apis/api/dataproc/overview) was involved in the process of enabling dataproc API (if you have more than one project you may need to switch to the correct one using the picker on the page next to the Google logo). Ignore any warnings you see about a need to get credentials. 
 6. Spin up a cluster (replace $PROJECTNAME with the project name you used above):  
`gcloud dataproc clusters create cluster-1 --zone us-east1-b --master-machine-type n1-standard-2 --master-boot-disk-size 150 --num-workers 3 --worker-machine-type n1-highmem-2 --worker-boot-disk-size 25 --project `**`$PROJECTNAME`**` --properties yarn:yarn.log-aggregation-enable=true,yarn:yarn.scheduler.maximum-allocation-mb=10240,yarn:yarn.nodemanager.resource.memory-mb=10240`

 7. Once this completes run `gcloud compute config-ssh`. This adds entries to your `~/.ssh/config` which allow you to connect to your cluster nodes. To see the list look at your `~/.ssh/config` file. An example: 

        Host cluster-1-m.us-east1-b.pirkongcpexample
	        HostName 105.126.210.81  
	        IdentityFile /Users/pirkdev/.ssh/google_ed25519  
	        UserKnownHostsFile=/Users/pirkdev/.ssh/google_compute_known_hosts  
	        HostKeyAlias=compute.1295540156620891161  
	        IdentitiesOnly=yes  
	        CheckHostIP=no 

    To SSH to this node I type `ssh -D 10010 cluster-1-m.us-east1-b.pirkongcpexample` (the `-m` indicates the master). (The `-D 10010` flag is optional and enables a SOCKS proxy you can configure a web browser with to see the [web interfaces](https://cloud.google.com/dataproc/concepts/cluster-web-interfaces))
  
 8. On GCP the default property `spark.home = /usr` is incorrect. Since `/root/` isn't accessible 
 (and thus putting an additional properties file in `local.pirk.properties.dir=/root/` isn't viable) one solution is to modify the
 compiled-in `pirk.properties`
 to have `local.pirk.properties.dir=/home/pirkdev/share` instead. (You'll want to change the `pirkdev` to your username on the node).
 At `/home/pirkdev/share/sparkfix.properties` put a file containing `spark.home=/usr/lib/spark/bin`
 9. Transfer your compiled jar to the cluster: e.g. `scp apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar cluster-1-m.us-east1-b.pirkongcpexample:~`
 10. Run your jar. For example: 
 
            hadoop jar apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar org.apache.pirk.test.distributed.DistributedTestDriver -j $PWD/apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar
            
            spark-submit --class org.apache.pirk.test.distributed.DistributedTestDriver  $PWD/apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar -j $PWD/apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar -t 1:JS

 11. When you want to stop your cluster: 
 
            [pirkdev:~] 2 % gcloud compute instances list
            NAME           ZONE        MACHINE_TYPE    …
            cluster-1-m    us-east1-b  n1-standard-2   …
            cluster-1-w-0  us-east1-b  n1-highmem-2    …
            cluster-1-w-1  us-east1-b  n1-highmem-2    …
            cluster-1-w-2  us-east1-b  n1-highmem-2    …
            [pirkdev:~] 2 % gcloud compute instances stop cluster-1-m  cluster-1-w-0 cluster-1-w-1 cluster-1-w-2

 Stop your instances to save money. [To quote google](https://cloud.google.com/compute/docs/instances/stopping-or-deleting-an-instance): 

> A stopped instance does not incur charges, but all of the resources that are attached to 
> the instance will still be charged. For example, you are charged for persistent disks and 
> external IP addresses according to the price sheet, even if an instance is stopped. To stop 
> being charged for attached resources, you can reconfigure a stopped instance to not use 
> those resources, and then delete the resources.


## Microsoft Azure
Right now Pirk can't be run on Microsoft's Azure HDInsight Hadoop platform because it only supports Java 7. Committer Jacob Wilder emailed a Microsoft engineer who works on HDInsight and heard that it is on Microsoft's roadmap for the end of September or October 2016. 

### Steps that will likely eventually work

These directions are based on the [basic cli instructions](https://azure.microsoft.com/en-us/documentation/articles/xplat-cli-install/) and the article [Create Linux-based clusters in HDInsight using the Azure CLI](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-hadoop-create-linux-clusters-azure-cli/). 

A note on HDInsight pricing: 
> HDInsight clusters billing is pro-rated per minute, whether you are using them or not. Please be sure to delete your cluster after you have finished using it. For information on deleting a cluster, see [How to delete an HDInsight cluster](https://azure.microsoft.com/en-us/documentation/articles/hdinsight-delete-cluster/).

1. Create a Microsoft Azure account and either add billing information or get some credit for it. 
2. If you haven't used Azure before then deploy [this template](https://github.com/Azure/azure-quickstart-templates/blob/master/101-acs-mesos/docs/CommonIssues.md). It will set up your account with licenses for the right resources. Don't forget to delete it after deploy. 
3. Install the Azure CLI and run `azure login` to authenticate.
4. Enter resource manager mode with `azure config mode arm`
5. Pick a location from the location list (`azure location list`, e.g. `eastus`). Remainder of this tutorial uses location eastus but you can switch it out. 
6. Create the cluster group. This example uses the name `pirkcluster1`, you can pick a different name.  `azure group create `**`pirkcluster1`**` eastus``
7. Create storage to use for the cluster `azure storage account create -g `**`pirkcluster1`**` --sku-name RAGRS -l eastus --kind Storage `**`pirkstorage1`**
8. Get one of the access keys for the storage account. `key1` is fine. 

        % azure storage account keys list -g pirkcluster1 pirkstorage
        info:    Executing command storage account keys list
        + Getting storage account keys                                
        data:    Name  Key                                Permissions
        data:    ----  ----------------------------------------------
        data:    key1  [a bunch of base64, save THIS]     Full  
        data:    key2  another bunch of base 64           Full       
        info:    storage account keys list command OK

9. Register for the Azure HDInsight provider: `azure provider register Microsoft.HDInsight`
10. Create the cluster. Replace the bold faced values. You already have `key1_from_above_command`. Select your own ssh and http passwords. In this example **`pirkhdinsight1`** is the name that will be used to SSH into the cluster and manage it.  
    `azure hdinsight cluster create -g pirkcluster1 -l eastus -y Linux --clusterType Hadoop --defaultStorageAccountName pirkstorage1.blob.core.windows.net --defaultStorageAccountKey `**`key1_from_above_command`**` --defaultStorageContainer `**`pirkhdinsight1`**` --workerNodeCount 2 --userName admin --password `**`httppassword`**` --sshUserName sshuser --sshPassword `**`sshpassword`**` `**`pirkhdinsight1`**
    
11. This command takes about 15 minutes. Once it finishes you can log into your cluster using `ssh sshuser@`**`pirkhdinsight1`**`-ssh.azurehdinsight.net` where you've replaced **`pirkhdinsight1`** with the name of your cluster. 
12. You may choose to install your ssh keys at this point using a command like `ssh-copy-id -i ~/.ssh/azure_ed25519 -o PubkeyAuthentication=no sshuser@pirkhdinsight1-ssh.azurehdinsight.net`
13. At this point you can't do anything since HDInsight doesn't support Java 8. Delete your cluster and wait for Azure HDInsight to support Java 8. 


## Amazon Web Services EC2 EMR
1. You'll need to have an AWS account with credit or billing information. 
2. Either create a key pair within the AWS user interface or make one locally and upload the public key. Note the exact name of the keypair in the AWS interface because it is an argument to later commands. The keypair used in this tutorial is called `amazon_rsa` in the amazon user interface and the private key is located on the local machine at `~/.ssh/amazon_rsa`
3. Install the [AWS CLI](https://aws.amazon.com/cli/) (probably using `pip install aws`) and run `aws configure` and input the required Access Key ID and Secret associated with your account. 
4. Run `aws emr create-default-roles`. 
5. Before you can create a cluster you need to make a JSON file locally. Call it (for example) `aws-cluster-conf.json` with these contents:
  
        [
          {
            "Classification": "yarn-site",
            "Properties": {
              "yarn.nodemanager.aux-services": "mapreduce_shuffle",
              "yarn.nodemanager.aux-services.mapreduce_shuffle.class": "org.apache.hadoop.mapred.ShuffleHandler"
            }
          }
        ]
    
    This configuration file fixes some YARN configuration options that (left absent)
    prevent distributed Pirk from running. 

6. Create the cluster:

        aws emr create-cluster \
           --name "Spark Cluster" \
           --release-label emr-5.0.0 \
           --applications Name=Spark \
           --ec2-attributes KeyName=amazon_rsa \
           --instance-type m3.xlarge \
           --instance-count 3 \
           --use-default-roles \
           --configurations file://./aws-cluster-conf.json
    
    Make note of the ClusterID it returns. For the remainder of these steps, assume that **`$cid`** has been set equal to the cluster ID (you may find it convenient to do this using `export cid=YOURCLUSTERID`)
7. Wait for your cluster to be ready. You might find this command helpful: `watch -n 60 aws emr describe-cluster --output json --cluster-id `**`$cid`**
8. Once your cluster is ready, go into the [AWS console in your browser](https://console.aws.amazon.com/ec2/v2/home) and add a firewall rule enabling SSH access. Select the correct region in the upper corner, then click on Security Groups in the left hand column. Find the row with the Group Name "ElasticMapReduce-master", select the Inbound tab in the lower pane, click Edit, and then add a Rule for SSH (in the drop down menu) with Source "My IP" (change this to another value if desired).
9. Upload the jar file (underneath the covers this is running `scp`):  
`aws emr put --cluster-id `**`$cid`**` --key-pair-file `**`~/.ssh/amazon_rsa`**` --src apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar`
10. SSH in using  
`aws emr ssh --cluster-id `**`$cid`**` --key-pair-file `**`~/.ssh/amazon_rsa`**  
If you want to SSH in and set up a SOCKS proxy to access the [web interfaces](https://docs.aws.amazon.com/ElasticMapReduce/latest/DeveloperGuide/emr-web-interfaces.html) (like in the GCP instructions) copy the output of the SSH command and add the `-D $SOCKSPORTNUM` flag. The YARN resource manager is on port 8088 of the Master node. 

11. Now on the cluster, you can run the distributed tests:  
`hadoop jar apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar org.apache.pirk.test.distributed.DistributedTestDriver -j $PWD/apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar -t 1:J`
12. When you are done working with your cluster, terminate it:  
`aws emr terminate-clusters --cluster-ids `**`$cid`**


## IBM Bluemix
1 .[Sign-up](https://console.ng.bluemix.net/registration/) for a free Bluemix account.
2. From the Bluemix [catalog](https://console.ng.bluemix.net/catalog/) open the "Big Insights for Apache Hadoop" service, found in the Data and Analytics section, and click "Create".  The basic service plan is free during beta, but will need to be recreated every two weeks.
3. Click "Open" to see the cluster list, and from there create a new cluster,
e.g.
  cluster name: test-cluster
  user name: pirk
  password: <password>.
In Configuration:
  Increase the number of data nodes to 5 (the maximum number available on the basic plan)
  Scroll down and select "Spark" as an optional component.
Click "Create".
4. Select the test cluster from the Cluster List and take note of the SSH host name, e.g. bi-hadoop-prod-4174.bi.services.us-south.bluemix.net
5. Now you can run the distributed tests by copying the Pirk jar file and executing it in Bluemix., i.e.
`$ scp target/apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar pirk@bi-hadoop-prod-4174.bi.services.us-south.bluemix.net:
pirk@bi-hadoop-prod-4174.bi.services.us-south.bluemix.net's password: 
apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar                                                       100%  145MB  10.3MB/s   00:14 
$ ssh pirk@bi-hadoop-prod-4174.bi.services.us-south.bluemix.net
pirk@bi-hadoop-prod-4174.bi.services.us-south.bluemix.net's password: 
-bash-4.1$ hadoop jar apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar org.apache.pirk.test.distributed.DistributedTestDriver -j apache-pirk-0.2.0-incubating-SNAPSHOT-exe.jar`

