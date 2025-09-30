# Project Setups 

## Prereqs
- **R** installed 
- (If you want to render the Quarto site) **Quarto CLI** installed: `quarto --version`.

## Clone and open project root

```bash
git clone <REPO_URL>
cd <REPO_FOLDER>  
```

## Restore the R environment

In an R console started from the project root:

```r
install.packages("renv", repos = "https://cloud.r-project.org")  # To install renv for first time only
renv::restore()  # installs packages listed in renv.lock into a project-local library
```

## Publish To Github Pages

```bash
quarto publish gh-pages
```