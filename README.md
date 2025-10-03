# Notes for Collaborators 

The following commands can be run from a console and terminal in R studio or via other means. 
The basic idea is the same no matter where you run your project. 

## Prereqs
- **R** installed 
- (If you want to render the Quarto site) **Quarto CLI** installed: `quarto --version`.

## Clone and open project root (from Github)
The exact commands will be found in Github. See the green Code button for the clone command and remote url.

```bash
git clone <REPO_URL>
cd <REPO_FOLDER>  
```

## Restore the R environment

Follow these instructions to get the right R packages. 

In an R console started from the project root:

```r
install.packages("renv", repos = "https://cloud.r-project.org")  # To install renv for first time only
renv::restore()  # installs packages listed in renv.lock into a project-local library
```

## Adding new R packages 
If you need to add new packages make sure to add them to the renv.lock. Make sure you are in the project 
directory and that the renv is activate. This should have been done already above. 

Use this code to install and updat the renv.lock file. 

```bash
renv::install(c("some-package"))
renv::snapshot(prompt = FALSE) # update the lock
```

Make sure the renv files (especially the renv.lock) get commited to git. 

## Publish To Github Pages

If you need to manually push to Github Pages use the following command:

```bash
quarto publish gh-pages
```

This will push the quarto site to Github. 