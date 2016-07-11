---
title: Apache Pirk (incubating)
skiph1fortitle: true
nav: nav_index
legal_notice: Apache Pirk (incubating), Apache, and the Apache feather logo, are trademarks of the [Apache Software Foundation](https://www.apache.org).
---

<div class="row">
  <div class="col-md-2" id="sidebar">
  <br>
    {% capture social-include %}{% include social.md %}{% endcapture %}{{ social-include | markdownify }}
    <hr>
    <a href="{{ site.baseurl }}/news"> News</a>
    <br>
    <br>
    {% capture newsfeed-include %}{% include newsfeed.md %}{% endcapture %}{{ newsfeed-include | markdownify }}
    <hr>
    <br>
    <a id="apache_incubator_logo" href="http://incubator.apache.org/"><img alt="Apache Incubator" class="img-responsive" src="{{ site.baseurl }}/images/apache-incubator-logo.png"></a>
    <br>
    <p style="text-align:center"><img src="{{ site.baseurl }}/images/tapir.png" width="60"/></p>
  </div>
  <div class="col-md-8 col-md-offset-1">
  <br>
  <img alt="Apache Pirk" class="img-responsive" src="{{ site.baseurl }}/images/pirkImage.png" width="500" style="margin:0 auto;"/>
  <br>
  <br>
    <div class="jumbotron" style="text-align:center">
      <font size="5"><b>Apache Pirk (incubating) is a framework for scalable <br> Private Information Retrieval (PIR).</b></font>
      <br>
      <br>
       <a class="btn btn-success" href="downloads/" role="button"><span class="glyphicon glyphicon-download"></span> Get Pirk</a>
    </div>
  </div>
</div>

## What is Pirk?

Pirk is a framework for scalable Private Information Retrieval (PIR). The goal of Pirk is to provide a landing place for robust, scalable, and practical implementations of PIR algorithms.

## Pirk Basics 

[Private Information Retrieval](https://en.wikipedia.org/wiki/Private_information_retrieval) (PIR) enables a user/entity to privately and securely obtain information from a dataset, to which they have been granted access, without revealing, to the dataset owner or to an observer, any information regarding the questions asked or the results obtained. Employing homomorphic encryption techniques, PIR enables datasets to remain resident in their native locations while giving the ability to query the datasets with sensitive terms.

There are two parties in a PIR transaction - the Querier, the party asking encrypted questions, and the Responder, the party holding the target data and answering encrypted questions (performing encrypted queries).  

Pirk is centered around the [Querier]({{ site.baseurl }}/for_users#querier) and the [Responder]({{ site.baseurl }}/for_users#responder).

In Pirk, the Querier is responsible for the following:

* Generating the encrypted query vectors (representing encrypted questions)
* Generating the necessary decryption items for each query vector
* Decrypting encrypted query results obtained from the Responder

Once the Querier generates an encrypted query, it must be sent to the Responder. 

In Pirk, the Responder is responsible for the following:

* Performing encrypted queries over their target datasets (using encrypted query vectors)
* Forming the encrypted query results 

The encrypted query results must be sent from the Responder back to the Querier for decryption.

Pirk employs generic [data and query schemas]({{ site.baseurl }}/for_users#data-and-query-schemas), specified via XML files, that are shared between the Querier and Responder to enable flexible and varied data and query types.

## Pirk Framework and Algorithms

The Pirk framework is centered around the Querier and Responder; key supporting elements include generic query and data schemas, encryption/decryption, query/response, and in-memory and distributed testing components. 

Pirk is seeded with the Wideskies PIR algorithm which employs [Paillier homomorphic encryption]({{ site.baseurl }}/papers/1999_asiacrypt_paillier_paper.pdf); the Wideskies white paper can be found [here]({{ site.baseurl }}/papers/wideskies_paper.pdf).

## Getting Started

Pirk can be downloaded [here]({{ site.baseurl }}/downloads)

Pirk is written in Java and uses a Maven build system. Dependencies can be found in the pom.xml file and include [Apache Hadoop](http://hadoop.apache.org/), [Apache Spark](http://spark.apache.org/),  and [Elasticsearch](https://github.com/elastic/elasticsearch). Currently, Pirk may be utilized in a distributed Hadoop/MapReduce or Spark framework as well as in standalone mode.

Pirk is seeded with the Wideskies PIR algorithm; please check out the [white paper]({{ site.baseurl }}/papers/wideskies_paper.pdf)

If you are a User, please check out the [For Users]({{ site.baseurl }}/for_users) section.

If you are a Developer, please check out the [For Developers]({{ site.baseurl }}/for_developers) section.

## Community

Please check out our [community]({{ site.baseurl }}/get_involved) section.

## Roadmap

Please check out our [Roadmap]({{ site.baseurl }}/roadmap).

## Disclaimer

Apache Pirk (incubating) is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the name of Apache TLP sponsor. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.
 