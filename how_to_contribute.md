---
title: How to Contribute
nav: nav_contribute
---

1. [Contribution Philosophy](#contribution-philosophy)
2. [General Process](#general-process)
3. [GitHub Pull Requests](#github-pull-requests)
	* [Git Setup](#git-setup)
	* [Creating a Pull Request](#creating-a-pull-request)
	* [Merging a Pull Request](#merging-a-pull-request)
	* [Closing a Pull Request Without Committing](#closing-a-pull-request-without-committing)
	* [Apache and GitHub Integration](#apache-and-github-integration)

## Contribution Philosophy

We welcome contributors of all kinds within the Apache Pirk community! 

A successful project requires many people to play many roles. Some members write code or documentation, while others are valuable as testers, submitting patches and suggestions. We also highly encourage the involvement of algorithmic team members who provide/discuss algorithmic ideas via the [dev mailing list]({{ site.baseurl }}/mailing_list_pirk) and who may or may not participate in their implementation.

If you have algorithmic ideas or would like to discuss a potential implementation item, please reach out to the community via [dev@pirk.incubator.apache.org](mailto:dev@pirk.incubator.apache.org) - we would love to hear from you! 
	
## General Process

The general process for code contribution is as follows:

1. Create a [Pirk JIRA issue](https://issues.apache.org/jira/browse/PIRK)
2. Fork the [Pirk source code](https://github.com/apache/incubator-pirk) from your personal GitHub repository and clone locally
3. Modify the source code to add your awesome features; you will most likely be modifying the apache/incubator-pirk master branch (unless you are [modifying the Pirk website]({{ site.baseurl }}/website_updates))
4. Ensure that all unit and functional tests pass
5. Ensure that your code follows the [Pirk code standards]({{ site.baseurl }}/for_developers#coding-standards)
6. Commit the changes to your local repository
7. Push the code back up to your GitHub fork of apache/incubator-pirk
8. [Create a Pull Request](#github-pull-requests) to the to [apache/incubator-pirk](https://github.com/apache/incubator-pirk) repository on GitHub. Please include the corresponding JIRA issue number and description in the title of the pull request: PIRK-XXXX: < JIRA-Issue-Description >
9. Members of the Pirk community can then comment on the pull request; be sure to watch for comments, respond, and make any necessary changes.
10. A Pirk committer will merge the pull request


## GitHub Pull Requests

All changes to the Apache Pirk codebase should go through a [GitHub pull request](https://help.github.com/articles/using-pull-requests/). 

(Thanks to [Apache Mahout](http://mahout.apache.org/) for sharing [their great pull request overview](http://mahout.apache.org/developers/github.html) with us!)

### Git Setup

Fork [apache/incubator-pirk](https://github.com/apache/incubator-pirk) to your personal GitHub repository. Clone this fork locally to set up "origin" to point to your remote fork on GitHub as the default remote (for the sake of this explanation); if you perform 

	git push origin master
	
it will go to your GitHub fork of apache/incubator-pirk. Please make sure that you follow branch naming conventions that will integrate with Apache Pirk JIRA issue numbers. For the issue number XXXX, please name branch containing the corresponding work PIRK-XXXX (all caps):

	git checkout -b PIRK-XXXX
	#do some work on the branch
	git commit -a -m "did some work"
	git push origin PIRK-XXXX #pushing to your fork of apache/incubator-pirk

Clone the Apache Pirk git repo via 

	git clone -o apache https://git-wip-us.apache.org/repos/asf/incubator-pirk.git
	
or attach via the following:

	git remote add apache https://git-wip-us.apache.org/repos/asf/incubator-pirk.git
	
Once you are ready to commit to the apache remote, you can create a pull request.

### Creating a Pull Request

Pull requests are made to the [apache/incubator-pirk](https://github.com/apache/incubator-pirk) repository on GitHub; please check out GitHub's info on [how to create a pull request](https://help.github.com/articles/creating-a-pull-request/).

First, push your work from your local PIRK-XXXX branch to the remote PIRK-XXXX branch of your apache/incubator-pirk GitHub fork:

	git checkout PIRK-XXXX
	git push origin PIRK-XXXX
	
Go to your PIRK-XXXX branch on GitHub. Since you forked it from GitHub's [apache/incubator-pirk](https://github.com/apache/incubator-pirk), it will default any pull request to go to the master branch of apache/incubator-pirk.

Click the "Compare, review, and create pull request" button.

You can edit the "to" and "from" for the pull request if it isn't correct. The "base fork" should be apache/incubator-pirk. The "base" will be master unless your are [updating the website]({{ site.baseurl }}/website_updates). The "head fork" will be your forked repo and the "compare" will be your PIRK-XXXX branch.

Click the "Create pull request" button and name the request "PIRK-XXXX", all caps. This will connect the comments of the pull request to the mailing list and JIRA comments.

From now on, the pull request lives in GitHub's [apache/incubator-pirk](https://github.com/apache/incubator-pirk); use the commenting UI there. The pull request is tied to the corresponding branch in your forked repo, so you can respond to comments, make fixes, and commit them from there; they will appear on the pull request page and be mirrored to JIRA and the mailing list.

All pull requests to [apache/incubator-pirk](https://github.com/apache/incubator-pirk) are built through [Pirk's Travis CI integration](https://travis-ci.org/apache/incubator-pirk). 


### Merging a Pull Request

Check out the GitHub info on [how to merge a pull request](https://help.github.com/articles/merging-a-pull-request/#merging-locally).

Pull requests are equivalent to a remote GitHub branch with (potentially) a multitude of commits. Please squash the remote commit history to have one commit per issue, rather than merging in all of the contributor's commits: 

	git pull --squash <repo> <branch>

Merging pull requests are equivalent to a "pull" of a contributor's branch; to merge the pull request branch PIRK-XXXX:

	git checkout master      # switch to local master branch
	git pull apache master   # fast-forward to current remote HEAD
	git pull --squash <contributor's GitHub repo> PIRK-XXXX  # merge to master
	
You now can commit the request:

	git commit --author "contributor_name <contributor_email>" -a -m "PIRK-XXXX <description> - closes apache/incubator-pirk#ZZ"
	
PIRK-XXXX is in all caps and ZZ is the pull request number on apache/incubator-pirk. Including "closes apache/incubator-pirk#ZZ" will close the [pull request automatically](https://help.github.com/articles/closing-issues-via-commit-messages/).

Next, push to the apache master branch:

	git push apache master
	
(this will require Apache handle credentials).

The pull request, once pushed, will get mirrored to apache/incubator-pirk on GitHub. 

### Closing a Pull Request Without Committing

To reject pull request ZZ (close without committing), issue an empty commit on master's HEAD without merging the pull request:

	git commit --allow-empty -m "closes apache/incubator-pirk#ZZ"
	git push apache master
	

### Apache and GitHub Integration

Check out Apache INFRA's blog post on [Apache-GitHub integration](https://blogs.apache.org/infra/entry/improved_integration_between_apache_and). 

Comments and pull requests with Pirk issue handles of the form

	PIRK-XXXX
	
should post to the mailing lists and JIRA. Please file a JIRA issue first, and then create a pull request with the description

	PIRK-XXXX: <jira-issue-description>
	
so that all subsequent comments will be automatically copied to JIRA.
