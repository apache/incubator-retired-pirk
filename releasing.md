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
* [How to Roll Back a Release Candidate](#how-to-roll-back-a-release-candidate)
* [Generate the Release Notes](#generate-the-release-notes)
3. [Vote on the Release](#vote-on-the-release)
4. [Publish the Release](#publish-the-release)
5. [Update the Website](#update-the-website)


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

*If the checks pass*, Nexus will close the repository and give a URL to the closed staging repo (which contains the candidate artifacts). Send this URL to folks in the voting email so that they can find the staged candidate release artifacts.

*If the checks do not pass*, follow the instructions on how to [roll back the release candidate](#how-to-roll-back-a-release-candidate), fix the issues, and start over with [creating the candidate artifacts](#create-the-candidate-release-artifacts).


### How to Roll Back a Release Candidate ###

A release candidate must be rolled back in Nexus and locally. 

To roll back the release candidate in Nexus, drop the staging repo by clicking the 'Drop' icon.

To roll back the release locally in your candidate relesae branch:

1. `mvn -Psigned_release versions:set -DnewVersion=<previous release version>-Snapshot`
2. `mvn -Psigned_release versions:commit`
3. `git commit` to commit the new pom version
4. `git push --delete apache <tagname>` to delete the remote github tag
5. `git tag -d <tagname>` to deletes the local tag


### Generate the Release Notes ###

To generate the release notes within via [Pirk JIRA](https://issues.apache.org/jira/browse/PIRK), follow the instructions found [here](https://confluence.atlassian.com/jira061/jira-administrator-s-guide/project-management/managing-versions/creating-release-notes).

Include the link to the release notes in the voting email. 

## Vote on the Release ##

To vote on a candidate release, send an email to the [dev list](mailto:dev@pirk.apache.incubator.org) with subject `[VOTE]: Pirk <release version> Release` and a body along the lines of: 

	This is the vote for <release version> of Apache Pirk (incubating).
	
	The vote will run for at least 72 hours and will close on <closing date>.
	
	The artifacts can be downloaded here: https://repository.apache.org/content/repositories/<repository name>
	
	All JIRAs completed for this release are tagged with 'FixVersion = <release version>'. You can view them here: <insert link to the JIRA release notes>
	
	The artifacts have been signed with Key : <ID of signing key>
	
	Please vote accordingly:
	
	[ ] +1, accept RC as the official <release version> release 
	[ ] -1, do not accept RC as the official <release version> release because...


## Publish the Release ##


## Update the Website ##

Add the current release link to the [Downloads page]({{ site.baseurl }}/downloads).

Update the javadocs. 





