---
title: Verifying a Release
nav: nav_verify_release
---

This guide for the verification of a release candidate is meant to encapsulate
the requirements of the PMC set forth by the ASF.

Verification of a release candidate can be broken down into three categories.

## Pirk Correctness ##

Pirk contains unit and integration tests which can be automatically run via Maven. These tests can be invoked by issues the following commands:

    $ mvn verify

Additionally, Pirk contains multiple distributed tests which must be run and must pass on a live cluster:

	hadoop jar <pirkJar> org.apache.pirk.test.distributed.DistributedTestDriver -j <full path to pirkJar>


## Foundation Level Requirements ##

The ASF requires that all artifacts in a release are cryptographically signed and distributed with hashes.

OpenPGP is an asymmetric encryption scheme which lends itself well to the globally distributed nature of Apache.
Verification of a release artifact can be done using the signature and the release-maker's public key. Hashes
can be verified using the appropriate command (e.g. `sha1sum`, `md5sum`).

An Apache release must contain a source-only artifact. This is the official release artifact. While a release of
an Apache project can contain other artifacts that do contain binary files. These non-source artifacts are for
user convenience only, but still must adhere to the same licensing rules.

PMC members should take steps to verify that the source-only artifact does not contain any binary files. There is
some leeway in this rule. For example, test-only binary artifacts (such as test files or jars) are acceptable as long
as they are only used for testing the software and not running it.

The following are the aforementioned Foundation-level documents provided for reference:

* [Applying the Apache Software License][2]
* [Legal's license application guidelines][3]
* [Common legal-discuss mailing list questions/resolutions][4]
* [ASF Legal Affairs Page][5]

## Apache Software License Application ##

Application of the Apache Software License v2 consists of the following steps on each artifact in a release. It's
important to remember that for artifacts that contain other artifacts (e.g. a tarball that contains JAR files or
an RPM which contains JAR files), both the tarball, RPM and JAR files are subject to the following roles.

The difficulty in verifying each artifact is that, often times, each artifact requires a different LICENSE and NOTICE
file. 

### LICENSE file ###

The LICENSE file should be present at the top-level of the artifact. This file should be explicitly named `LICENSE`,
however `LICENSE.txt` is acceptable but not preferred. This file contains the text of the Apache Software License 
at the top of the file. At the bottom of the file, all other open source licenses _contained in the given
artifact_ must be listed at the bottom of the LICENSE file. Contained components that are licensed with the ASL themselves
do not need to be included in this file. It is common to see inclusions in file such as the MIT License of 3-clause
BSD License.

### NOTICE file ###

The NOTICE file should be present at the top-level of the artifact beside the LICENSE file. This file should be explicitly
name `NOTICE`, while `NOTICE.txt` is also acceptable but not preferred. This file contains the copyright notice for
the artifact being released. As a reminder, the copyright is held by the Apache Software Foundation, not the individual
project.

The second purpose this file serves is to distribute third-party notices from dependent software. Specifically, other code
which is licensed with the ASLv2 may also contain a NOTICE file. If such an artifact which contains a NOTICE file is
contained in artifact being verified for releases, the contents of the contained artifact's NOTICE file should be appended
to this artifact's NOTICE file. 

[2]: https://www.apache.org/dev/apply-license
[3]: https://www.apache.org/legal/src-headers
[4]: https://www.apache.org/legal/resolved
[5]: https://www.apache.org/legal
