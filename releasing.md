---
title: Making a Release
nav: nav_releasing
---

This is a guide to making a release of Apache Pirk (incubating). Please follow the steps below.


1. [Preliminaries](#preliminaries)
* [Apache Release Documentation](#apache-release-documentation)
* [Code Signing Key](#code-signing-key)
* [Prepare Your Maven Settings](#prepare-your-maven-settings)
* [Ensure JIRA Issues are Appropriately Tagged for the Release](#ensure-jira-issues-are-appropriately-tagged-for-the-release)
2. [Create the Release Candidate](#create-the-release-candidate)
* [Create the Candidate Release Branch](#create-the-candidate-release-branch)
* [Create the Candidate Release Artifacts](#create-the-candidate-release-artifacts)
* [Validate the Release Candidate](#validate-the-release-candidate)
* [Close the Staging Repository](#close-the-staging-repository)
* [Copy Artifacts to the Apache Distribution Dev Repository](#copy-artifacts-to-the-apache-distribution-dev-repository)
* [How to Roll Back a Release Candidate](#how-to-roll-back-a-release-candidate)
* [Generate the Release Notes](#generate-the-release-notes)
3. [Vote on the Release](#vote-on-the-release)
* [Apache Pirk Community Vote](#apache-pirk-community-vote)
* [Incubator PMC Vote](#incubator-pmc-vote)
4. [Publish the Release](#publish-the-release)
* [Publish the Maven Artifacts](#publish-the-maven-artifacts)
* [Publish the Artifacts to the Apache Release Repository](#publish-the-artifacts-to-the-apache-release-repository)
5. [Announce the Release](#announce-the-release)
6. [Update the Website](#update-the-website)


## Preliminaries ##

### Apache Release Documentation ###

The ASF release documentation, including the incubator release documentation, can be found here:

* [Apache Release Guide](http://www.apache.org/dev/release-publishing)
* [Apache Release Policy](http://www.apache.org/dev/release.html)
* [Apache Incubator Release Guidelines](http://incubator.apache.org/guides/releasemanagement.html)
* [Apache Incubator Release Policy](http://incubator.apache.org/incubation/Incubation_Policy.html#Releases)
* [Maven Release Info](http://www.apache.org/dev/publishing-maven-artifacts.html)

### Code Signing Key ###

Create a code signing gpg key for release signing; use `<your Apache ID>@apache.org` for your primary ID for the code signing key. See the [Apache Release Signing documentation](https://www.apache.org/dev/release-signing) for futher information.

* Add your code signing key to your Apache ID [here](https://id.apache.org/)
* Publish it to a [public key server](http://www.apache.org/dev/release-signing.html#keyserver) such as the [MIT key server](http://pgp.mit.edu/).
* Add it to the Pirk [KEYS file](https://github.com/apache/incubator-pirk/blob/master/KEYS).

### Prepare Your Maven Settings ###

Make sure that your Maven settings.xml file contains the following:

	<settings>
	   <profiles>
	         <profile>
	           <id>signed_release</id>
	           <properties>
	               <mavenExecutorId>forked-path</mavenExecutorId>
	               <gpg.keyname>yourKeyName</gpg.keyname>
	               <gpg.passphrase>yourKeyPassword</gpg.passphrase>
	               <deploy.altRepository>incubator-pirk.releases::default::https://dist.apache.org/repos/dist/dev/incubator/pirk/</deploy.altRepository>
               <username>yourApacheID</username>
               <deploy.url>https://dist.apache.org/repos/dist/dev/incubator/pirk/</deploy.url>
	           </properties>
	       </profile>
	 </profiles>
	  <servers>
	    	<server>
	      		<id>apache.releases.https</id>
	      		<username>yourApacheID</username>
	     		<password>yourApachePassword</password>
	   		 </server>
	         <server>
	             <id>repository.apache.org</id>
	             <username>yourApacheID</username>
	             <password>yourApachePassword</password>
	         </server>
	  </servers>
	</settings>


### Ensure JIRA Issues are Appropriately Tagged for the Release ###

Ensure that all Pirk JIRA issues that are addressed in this release are marked with the release version in the 'FixVersion' field of the issue.

## Create the Release Candidate ##

### Create the Candidate Release Branch ###

Create a branch off of master with its name equal to the release version. For example, if the candidate release version is 0.0.1, create a branch called 0.0.1.

### Create the Candidate Release Artifacts ###

Check out the candidate release branch. 

Ensure that the application builds and all in-memory tests pass via `mvn clean install`. 

Perform the following to generate and stage the artifacts:

1. `mvn clean release:clean`
2. `mvn release:prepare -Psigned_release -Darguments="-DskipTests"`
* You will be promted to answer the following questions:
* What is the release version for "Apache Pirk (incubating) Project"? *Answer:* `<new release version>-incubating`
* What is SCM release tag or label for "Apache Pirk (incubating) Project"? *Answer:* Accept the default
* What is the new development version for "Apache Pirk (incubating) Project"? *Answer:* Accept the default
3. `mvn -Psigned_release release:perform -Darguments="-DskipTests"`
* This command will generate the artifacts and push them to the [Nexus repo](https://repository.apache.org/#stagingRepositories). If you would like to perform a dry run first (without pushing the artifacts to the repo), add the arg `-DdryRun=true`

The candidate release artifacts can be found in the [Nexus staging repo](https://repository.apache.org/#stagingRepositories). Log in with your Apache creds; the candidate release artifacts are found in `orgapachepirk-1001` (the appended number may be different).

The candidate artifacts can also be found in the `target` folder of your local branch.

NOTE: If you are performing a source-only release, please remove all artifacts from the staging repo except for the .zip file containing the source and the javadocs jar file. In the Nexus GUI, you can right click on each artifact to be deleted and then select 'Delete'.

### Validate the Release Candidate ###

As per the Apache documentation, verify that the release candidate artifacts satisfy the following:

* Checksums and PGP signatures are valid
* Build is successful including automated tests
* DISCLAIMER is correct, filenames include "incubating"
* LICENSE and NOTICE files are correct and dependency licenses are acceptable
	* LICENSE and NOTICE files at the root of the artifact directory must only reflect the contents of the artifact in which they are contained.  
	* NOTE: 
		* The LICENSE and NOTICE files contained at the root of the Apache Pirk code repository correspond to the source code only. 
		* License and notice files corresponding to the binary distribution/artifacts of Apache Pirk are found in the `src/main/resources/META-INF/bin-license-notice` directory under `LICENSE-bin` and `NOTICE-bin`. The full licenses of third-party dependencies contained in Pirk's binary artifacts are located in the  `licenses` directory.
	* See:
		* [LICENSE file requirements](http://www.apache.org/dev/release.html#license)
		* [LICENSE requirements for distribution artifacts with multiple licenses](http://www.apache.org/dev/release.html#distributing-code-under-several-licenses)
		* [NOTICE file requirements](http://www.apache.org/dev/release.html#notice-content)
		* [Apache Legal](http://apache.org/legal/)
		* [Acceptable](http://www.apache.org/legal/resolved.html#category-a) and [Unacceptable](http://www.apache.org/legal/resolved.html#category-x) Dependency Licenses
* The Cryptographic Export Control section in the README is up-to-date
* All source files have license headers where appropriate, RAT checks pass
* The provenance of all source files is clear (ASF or software grants)

### Close the Staging Repository ###

If the release candidate appears to pass the validation checklist, close the staging repository in Nexus by selecting the staging repository `orgapachepirk-1001` and clicking on the 'Close' icon. 

Nexus will now run through a series of checksum and signature validations. 

*If the checks pass*, Nexus will close the repository and give a URL to the closed staging repo (which contains the candidate artifacts). Include this URL in the voting email so that folks can find the staged candidate release artifacts. Move on to [copy the artifacts via svn to the Apache distribution dev repository](#copy-artifacts-to-the-apache-distribution-dev-repository). 

*If the checks do not pass*, follow the instructions on how to [roll back the release candidate](#how-to-roll-back-a-release-candidate), fix the issues, and start over with [creating the candidate artifacts](#create-the-candidate-release-artifacts).

### Copy Artifacts to the Apache Distribution Dev-Repository ###

Via svn, copy the candidate release artifacts to [https://dist.apache.org/repos/dist/dev/incubator/pirk/](https://dist.apache.org/repos/dist/dev/incubator/pirk/)

	svn checkout https://dist.apache.org/repos/dist/dev/incubator/pirk/ --username=<your user name>

	mkdir pirk/<release version>
	
	copy the release artifacts into the new directory
	
	svn add <release version directory>

	svn commit -m 'adding candidate release artifacts' --username=<your user name>


### How to Roll Back a Release Candidate ###

A release candidate must be rolled back in Nexus, the Apache dist/dev repo, and locally. 

To roll back the release candidate in Nexus, drop the staging repo by clicking the 'Drop' icon.

To roll back the release candidate in [https://dist.apache.org/repos/dist/dev/incubator/pirk/](https://dist.apache.org/repos/dist/dev/incubator/pirk/):

	svn checkout https://dist.apache.org/repos/dist/dev/incubator/pirk/<release version> --username=<your user name>

	svn delete <release version directory>

	svn commit -m 'delete candidate release artifacts' --username=<your user name>

To roll back the release locally in your candidate release branch:

1. `mvn -Psigned_release versions:set -DnewVersion=<previous release version>-Snapshot`
2. `mvn -Psigned_release versions:commit`
3. `git commit` to commit the new pom version
4. `git push --delete apache <tagname>` to delete the remote github tag
5. `git tag -d <tagname>` to deletes the local tag


### Generate the Release Notes ###

To generate the release notes within via [Pirk JIRA](https://issues.apache.org/jira/browse/PIRK), follow the instructions found [here](https://confluence.atlassian.com/jira061/jira-administrator-s-guide/project-management/managing-versions/creating-release-notes).

Include the link to the release notes in the voting emails. 

## Vote on the Release ##

As per the Apache Incubator [release guidelines](http://incubator.apache.org/incubation/Incubation_Policy.html#Releases), all releases for incubating projects must go through a two-step voting process. [First](#apache-pirk-community-vote), release voting must successfully pass within the Apache Pirk community via the `dev@pirk.incubator.apache.org` mailing list. [Then](#incubator-pmc-vote), release voting must successfully pass within the Apache Incubator PMC via the `general@incubator.apache.org` mailing list.

General information regarding the Apache voting process can be found [here](http://www.apache.org/foundation/voting.html).

### Apache Pirk Community Vote

To vote on a candidate release, send an email to the [dev list](mailto:dev@pirk.apache.incubator.org) with subject `[VOTE]: Apache Pirk <release version> Release` and a body along the lines of: 

	This is the vote for <release version> of Apache Pirk (incubating).
	
	The vote will run for at least 72 hours and will close on <closing date>.
	
	The artifacts can be downloaded here: https://dist.apache.org/repos/dist/dev/incubator/pirk/release/<release number> or from the Maven staging repo here: https://repository.apache.org/content/repositories/<repository name>
	
	All JIRAs completed for this release are tagged with 'FixVersion = <release version>'. You can view them here: <insert link to the JIRA release notes>
	
	The artifacts have been signed with Key : <ID of signing key>
	
	Please vote accordingly:
	
	[ ] +1, accept RC as the official <release version> release 
	[ ] -1, do not accept RC as the official <release version> release because...
	
If any -1 (binding) votes are entered, then address them such that the voter changes their vote to a +1 (binding) or cancel the vote, [roll back the release candidate](#how-to-roll-back-a-release-candidate), fix the issues, and start over with [creating the candidate artifacts](#create-the-candidate-release-artifacts).

Once 72 hours has passed (which is generally preferred) and/or at least three +1 (binding) votes have been cast with no -1 (binding) votes, send an email closing the vote and pronouncing the release candidate a success.

	The Apache Pirk <release version> vote is now closed and has passed as follows:

	[number] +1 (binding) votes
	[number] -1 (binding) votes

	A vote Apache Pirk <release version> will now be called on general@incubator.apache.org.

### Incubator PMC Vote

Once the candidate release vote passes on dev@pirk.apache.incubator.org, call a vote on IMPC `general@incubator.apache.org` with an email a with subject `[VOTE]: Apache Pirk <release version> Release` and a body along the lines of:

	The PPMC vote for the Apache Pirk 0.1.0-incubating release has passed. We kindly request that the IPMC now vote on the release.

	The PPMC vote thread is located here: <link to the dev voting thread>

	The artifacts can be downloaded here: https://dist.apache.org/repos/dist/dev/incubator/pirk/release/<release number> 

	The artifacts have been signed with Key : <ID of signing key>

	All JIRAs completed for this release are tagged with 'FixVersion = <release version>'. You can view them here: <insert link to the JIRA release notes>
	
	Please vote accordingly:

	[ ] +1, accept as the official Apache Pirk <release number> release 
	[ ] -1, do not accept as the official Apache Pirk <release number> release because...

	The vote will run for at least 72 hours.

If any -1 (binding) votes are entered, then address them such that the voter changes their vote to a +1 (binding) or cancel the vote, [roll back the release candidate](#how-to-roll-back-a-release-candidate), fix the issues, and start over with [creating the candidate artifacts](#create-the-candidate-release-artifacts) (including re-voting within the Apache Pirk community on dev@pirk.apache.incubator.org).

Once 72 hours has passed (which is generally preferred) and/or at least three +1 (binding) votes have been cast with no -1 (binding) votes, send an email closing the vote and pronouncing the release candidate a success.

	The Apache Pirk <release version> vote is now closed and has passed as follows:

	[number] +1 (binding) votes
	[number] -1 (binding) votes

	The Apache Pirk (incubating) community will proceed with the release.

## Publish the Release ##

Once the Apache Pirk PPMC and IPMC votes both pass, publish the release artifacts to the [Nexus Maven repository](#publish-the-maven-artifacts) and to the [Apache release repository](#publish-the-artifacts-to-the-apache-release-repository). 

### Publish the Maven Artifacts

Release the Maven artifacts in Nexus by selecting the staging repository `orgapachepirk-1001` and clicking on the 'Release' icon. 

### Publish the Artifacts to the Apache Release Repository

Via svn, copy the candidate release artifacts to [https://dist.apache.org/repos/dist/release/incubator/pirk/](https://dist.apache.org/repos/dist/release/incubator/pirk/)

	svn checkout https://dist.apache.org/repos/dist/release/incubator/pirk/ --username=<your user name>

	mkdir pirk/<release version>
	
	copy the release artifacts into the new directory
	
	svn add <release version directory>

	svn commit -m 'adding release artifacts' --username=<your user name> 

## Announce the Release ##

Send an email to `announce@apache.org`, `general@incubator.apache.org`, and `dev@pirk.apache.incubator.org` with the subject `[ANNOUNCE] Apache Pirk <release number> Release` and a body along the lines of: 

	The Apache Pirk team would like to announce the release of Apache Pirk <release version>. 

	Apache Pirk (incubating) is a framework for scalable Private Information Retrieval (PIR). The goal of Pirk is to provide a landing place for robust, scalable, and practical implementations of PIR algorithms.

	More details regarding Apache Pirk can be found here:
	http://pirk.incubator.apache.org/

	The release artifacts can be downloaded here: 
	https://dist.apache.org/repos/dist/release/incubator/pirk/<release version>

	Maven artifacts have been made available here: 
	https://repository.apache.org/content/repositories/releases/org/apache/pirk/

	All JIRAs completed for this release are tagged with 'FixVersion = <release version>'; the release notes can be found here: 
	<link to release notes on JIRA>

	Thanks!

	The Apache Pirk Team
	
	--- DISCLAIMER  Apache Pirk is an effort undergoing incubation at the Apache Software Foundation (ASF), sponsored by the Apache Incubator PMC. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects.  While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

General Apache information regarding announcing a release may be found [here](http://www.apache.org/dev/release.html#release-announcements).

## Update the Website ##

Add the current release link to the [Downloads page]({{ site.baseurl }}/downloads).

Update the [javadocs]({{ site.baseurl }}/javadocs). 

Update the [News]({{ site.baseurl }}/news) page and the News info bar on the left side of the [homepage]({{ site.baseurl }}/) (via editing _includes/newsfeed.md) with the release information.





