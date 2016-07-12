---
title: Updating the Website
nav: nav_website_updates
---

Pirk's web site is developed using [Jekyll](https://jekyllrb.com). The Jekyll source is contained in the [gh-pages](https://git-wip-us.apache.org/repos/asf?p=incubator-pirk.git;a=tree;h=gh-pages) branch and the [asf-site](https://git-wip-us.apache.org/repos/asf?p=incubator-pirk.git;a=tree;h=asf-site) branch contains the HTML content.

Website development is
performed by editing the contents of the [gh-pages](https://git-wip-us.apache.org/repos/asf?p=incubator-pirk.git;a=tree;h=gh-pages) branch, either
directly by a committer, with a pull request to [GitHub](https://github.com/apache/incubator-pirk), or a patch
submitted to [JIRA](https://issues.apache.org/jira/browse/PIRK). The rendered site can be previewed locally or on
[GitHub](https://apache.github.io/incubator-pirk/), and the rendered site (in the `_site` directory) will be
merged into the `asf-site` branch to update the [official site](http://pirk.incubator.apache.org/) after being built with the `_config-asf.yml` configuration.

To manage any Gem dependencies, it is highly recommended to use [Bundler](https://bundler.io).
To start using Bundler, install it and then install the dependencies for the website:

    gem install bundler
    bundle install

To get help with jekyll:

    jekyll help

To test the site locally (usually on http://localhost:4000):

    bundle exec jekyll serve --config _config-asf.yml --safe

To build for updating the `asf-site` branch:

    bundle exec jekyll build --config _config-asf.yml --safe

A [post-commit hook](https://git-wip-us.apache.org/repos/asf?p=accumulo.git;a=blob_plain;f=_devtools/git-hooks/post-commit;hb=gh-pages) is available (Thanks [Apache Accumulo!](https://accumulo.apache.org/)) for you to automatically create a
commit in the `asf-site` branch locally each time you commit to the `gh-pages`
branch. You can also run this command manually:

    ./_devtools/git-hooks/post-commit

To automatically run this post-commit hook in your local repository, copy
the given file into your `.git/hook` directory:

    cp ./_devtools/git-hooks/post-commit .git/hooks/
    