# snake-repo

Python Data Science Project Template

## Setup local environment

`build` command from Makefile creates local environment for project development. The command performs following actions:

1. Create `pyenv` virtual environment with `python` version noted in Makefile
2. Install dependencies listed in requirement-dev.txt file
3. Setup `pre-commit` by installing dependencies listed in .pre-commit-config.yaml file
4. Install ipykernel for jupyter notebook environment

```
make build
```
