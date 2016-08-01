---
title: FAQ
nav: nav_faq
---

1. [Pirk Trust Model](#what-is-the-trust-model-for-pirk)

### What is the trust model for Pirk?

Apache Pirk is an application that runs within a user's system to provide the ability to (1) generate a secure query via PIR and/or (2) execute a secure query via PIR. A Pirk Querier generates a Query object and a Pirk Responder generates a Response object. For a user system that is running the Pirk application, these objects are just an output of the application.

Communication between the Querier and Responder entities is necessary for the Querier to send the Responder a query (Query object) and for the Responder to return the results (Response object), but those transport mechanisms are external to Pirk. User systems running the Pirk application can choose to communicate with each other in whatever way they would like to.

As such, the authentication of the Query and Response objects remain external to Pirk and is left as a part of the access control/authentication of users' systems that are running the Pirk application and communicating with each other. 

This also is of the same philosophy as the Responder's data access: the Responder can only execute a query over data to which the data owner has given it access. This is enforced outside of Pirk -- data access controls of the data owner for a data user (such as Pirk) are outside of the scope of the Pirk project.


