# Apex Documentation

Apex documentation repository for content available on http://apex.incubator.apache.org/docs/

Documentation is written in [Markdown](https://guides.github.com/features/mastering-markdown/) format and statically generated into HTML using [MkDocs](http://www.mkdocs.org/).  All documentation is located in the [docs](docs) directory, and [mkdocs.yml](mkdocs.yml) file describes the navigation structure of the published documentation.

## Authoring

New pages can be added under [docs](docs) or related sub-category, and a reference to the new page must be added to the [mkdocs.yml](mkdocs.yml) file to make it availabe in the navigation.  Embedded images are typically added to images folder at the same level as the new page.

When creating or editing pages, it can be useful to see the live results, and how the documents will appear when published.  Live preview feature is available by running the following command at the root of the repository:

```bash
mkdocs serve
```

For additional details see [writing your docs](http://www.mkdocs.org/user-guide/writing-your-docs/) guide.

## Site Configuration

Guides on applying site-wide [configuration](http://www.mkdocs.org/user-guide/configuration/) and [themeing](http://www.mkdocs.org/user-guide/styling-your-docs/) are available on the MkDocs site.

## Deployment

**Under Review**: Current deployment process is under review, and may change from the one outlined below.


Deployment is done from master branch of the repository by executing the following command:

```bash
mkdocs gh-deploy --clean
```

This results in all the documentation under [docs](docs) being statically generatd into HTML files and deployed as top level in [gh-pages](https://github.com/apache/incubating-apex-core/tree/gh-pages) branch.  For more details on how this is done see [MkDocs - Deploying Github Pages](http://www.mkdocs.org/user-guide/deploying-your-docs/#github-pages).