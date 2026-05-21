# infra.example

Reference Terraform module showing how wytchr expects its Postgres to be
provisioned (single Aiven for PostgreSQL service, outputs `database_url`).

These files are **not** used by any installer in this repo. The real
state-bearing copy lives with the deployer — for the homelab deploy that
is `~/homelab/compose/wytchr/infra/`. Copy this directory somewhere you
own, then point the installer at it:

```sh
./install.sh --infra-dir /path/to/your/infra
# or
INFRA_DIR=/path/to/your/infra ./install.sh
```

The installer never writes Terraform state inside this repo.
