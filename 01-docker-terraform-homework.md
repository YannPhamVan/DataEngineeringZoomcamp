
# Module 1 Homework: Docker & SQL

In this homework we'll prepare the environment and practice
Docker and SQL

When submitting your homework, you will also need to include
a link to your GitHub repository or other public code-hosting
site.

This repository should contain the code for solving the homework. 

When your solution has SQL or shell commands and not code
(e.g. python files) file format, include them directly in
the README file of your repository.


## Question 1. Understanding docker first run 

Run docker with the `python:3.12.8` image in an interactive mode, use the entrypoint `bash`.

What's the version of `pip` in the image?

- **24.3.1**
- 24.2.1
- 23.3.1
- 23.2.1

```bash
$ winpty docker run -it python:3.12.8 bash
Unable to find image 'python:3.12.8' locally
3.12.8: Pulling from library/python
e00350058e07: Download complete
5f16749b32ba: Download complete
eb52a57aa542: Download complete
Digest: sha256:9cdef3d6a7d669fd9349598c2fc29f5d92da64ee76723c55184ed0c8605782cc
Status: Downloaded newer image for python:3.12.8
root@067488efc63d:/# pip --version
pip 24.3.1 from /usr/local/lib/python3.12/site-packages/pip (python 3.12)
```

## Question 2. Understanding Docker networking and docker-compose

Given the following `docker-compose.yaml`, what is the `hostname` and `port` that **pgadmin** should use to connect to the postgres database?

```yaml
services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin  

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data
```

- postgres:5433
- localhost:5432
- db:5433
- postgres:5432
- **db:5432**

If there are more than one answers, select only one of them

##  Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from October 2019:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
```

You will also need the dataset with zones:

```bash
wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv
```

Download this data and put it into Postgres.

You can use the code from the course. It's up to you whether
you want to use Jupyter or a python script.

## Question 3. Trip Segmentation Count

During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, **respectively**, happened:
1. Up to 1 mile
```sql
SELECT
    COUNT(1)
FROM 
    green_tripdata t
WHERE
	t."trip_distance" <= 1;
```
2. In between 1 (exclusive) and 3 miles (inclusive),
```sql
SELECT
    COUNT(1)
FROM 
    green_tripdata t
WHERE
	t."trip_distance" <= 3
	AND t."trip_distance" > 1;
```
3. In between 3 (exclusive) and 7 miles (inclusive),
```sql
SELECT
    COUNT(1)
FROM 
    green_tripdata t
WHERE
	t."trip_distance" <= 7
	AND t."trip_distance" > 3;
```
4. In between 7 (exclusive) and 10 miles (inclusive),
```sql
SELECT
    COUNT(1)
FROM 
    green_tripdata t
WHERE
	t."trip_distance" <= 10
	AND t."trip_distance" > 7;
```
5. Over 10 miles
```sql
SELECT
    COUNT(1)
FROM 
    green_tripdata t
WHERE
	t."trip_distance" > 10;
```

Answers:

- 104,802;  197,670;  110,612;  27,831;  35,281
- 104,802;  198,924;  109,603;  27,678;  35,189
- 104,793;  201,407;  110,612;  27,831;  35,281
- 104,793;  202,661;  109,603;  27,678;  35,189
- **104,838;  199,013;  109,645;  27,688;  35,202**




## Question 4. Longest trip for each day

Which was the pick up day with the longest trip distance?
Use the pick up time for your calculations.

Tip: For every day, we only care about one single trip with the longest distance. 

- 2019-10-11
- 2019-10-24
- 2019-10-26
- **2019-10-31**
```sql
SELECT
    CAST(lpep_pickup_datetime AS DATE) AS "day",
    MAX(trip_distance) AS "max_dist"
FROM 
    green_tripdata t
GROUP BY
    CAST(lpep_pickup_datetime AS DATE)
ORDER BY
    "max_dist" DESC
LIMIT 100;
```


## Question 5. Three biggest pickup zones

Which were the top pickup locations with over 13,000 in
`total_amount` (across all trips) for 2019-10-18?

Consider only `lpep_pickup_datetime` when filtering by date.
 
- **East Harlem North, East Harlem South, Morningside Heights**
- East Harlem North, Morningside Heights
- Morningside Heights, Astoria Park, East Harlem South
- Bedford, East Harlem North, Astoria Park
```sql
SELECT
    CAST(lpep_pickup_datetime AS DATE) AS "day",
	SUM(total_amount) AS "sum",
	zpu."Zone" AS "PickUP_zone"
FROM 
    green_tripdata t LEFT JOIN zones zpu
		ON t."PULocationID" = zpu."LocationID"
WHERE
	DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY
	1, 3
HAVING
    SUM(total_amount) > 13000
ORDER BY
	"sum" DESC;
```

## Question 6. Largest tip

For the passengers picked up in October 2019 in the zone
named "East Harlem North" which was the drop off zone that had
the largest tip?

Note: it's `tip` , not `trip`

We need the name of the zone, not the ID.

- Yorkville West
- **JFK Airport**
- East Harlem North
- East Harlem South
```sql
SELECT
    CAST(lpep_pickup_datetime AS DATE) AS "day",
	tip_amount,
	zpu."Zone" AS "PickUP_zone",
	zdo."Zone" AS "DropOFF_zone"
FROM 
    green_tripdata t LEFT JOIN zones zpu
		ON t."PULocationID" = zpu."LocationID"
	LEFT JOIN zones zdo
		ON t."DOLocationID" = zdo."LocationID"
WHERE
	DATE(lpep_pickup_datetime) >= '2019-10-01'
	AND DATE(lpep_pickup_datetime) <= '2019-10-31'
	AND zpu."Zone" = 'East Harlem North'
ORDER BY
	tip_amount DESC
LIMIT 100;
```

## Terraform

In this section homework we'll prepare the environment by creating resources in GCP with Terraform.

In your VM on GCP/Laptop/GitHub Codespace install Terraform. 
Copy the files from the course repo
[here](../../../01-docker-terraform/1_terraform_gcp/terraform) to your VM/Laptop/GitHub Codespace.

Modify the files as necessary to create a GCP Bucket and Big Query Dataset.


## Question 7. Terraform Workflow

Which of the following sequences, **respectively**, describes the workflow for: 
1. Downloading the provider plugins and setting up backend,
2. Generating proposed changes and auto-executing the plan
3. Remove all resources managed by terraform`

Answers:
- terraform import, terraform apply -y, terraform destroy
- teraform init, terraform plan -auto-apply, terraform rm
- terraform init, terraform run -auto-approve, terraform destroy
- **terraform init, terraform apply -auto-approve, terraform destroy**
- terraform import, terraform apply -y, terraform rm

```bash
PS C:\terrademo> terraform init               
Initializing the backend...
Initializing provider plugins...
- Reusing previous version of hashicorp/google from the dependency lock file
- Using previously-installed hashicorp/google v6.17.0

Terraform has been successfully initialized!

You may now begin working with Terraform. Try running "terraform plan" to see
any changes that are required for your infrastructure. All Terraform commands
should now work.

If you ever set or change modules or backend configuration for Terraform,
rerun this command to reinitialize your working directory. If you forget, other
commands will detect it and remind you to do so if necessary.
PS C:\terrademo> terraform apply -auto-approve

Terraform used the selected providers to generate the following execution plan. Resource    
actions are indicated with the following symbols:
  + create

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be created
  + resource "google_bigquery_dataset" "demo_dataset" {
      + creation_time              = (known after apply)
      + dataset_id                 = "demo_dataset"
      + default_collation          = (known after apply)
      + delete_contents_on_destroy = false
      + effective_labels           = {
          + "goog-terraform-provisioned" = "true"
        }
      + etag                       = (known after apply)
      + id                         = (known after apply)
      + is_case_insensitive        = (known after apply)
      + last_modified_time         = (known after apply)
      + location                   = "US"
      + max_time_travel_hours      = (known after apply)
      + project                    = "noted-lead-448822-q9"
      + self_link                  = (known after apply)
      + storage_billing_model      = (known after apply)
      + terraform_labels           = {
          + "goog-terraform-provisioned" = "true"
        }

      + access (known after apply)
    }

  # google_storage_bucket.demo-bucket will be created
  + resource "google_storage_bucket" "demo-bucket" {
      + effective_labels            = {
          + "goog-terraform-provisioned" = "true"
        }
      + force_destroy               = true
      + id                          = (known after apply)
      + location                    = "US"
      + name                        = "noted-lead-448822-q9-terra-bucket"
      + project                     = (known after apply)
      + project_number              = (known after apply)
      + public_access_prevention    = (known after apply)
      + rpo                         = (known after apply)
      + self_link                   = (known after apply)
      + storage_class               = "STANDARD"
      + terraform_labels            = {
          + "goog-terraform-provisioned" = "true"
        }
      + uniform_bucket_level_access = (known after apply)
      + url                         = (known after apply)

      + lifecycle_rule {
          + action {
              + type          = "AbortIncompleteMultipartUpload"
                # (1 unchanged attribute hidden)
            }
          + condition {
              + age                    = 1
              + matches_prefix         = []
              + matches_storage_class  = []
              + matches_suffix         = []
              + with_state             = (known after apply)
                # (3 unchanged attributes hidden)
            }
        }

      + soft_delete_policy (known after apply)

      + versioning (known after apply)

      + website (known after apply)
    }

Plan: 2 to add, 0 to change, 0 to destroy.
google_bigquery_dataset.demo_dataset: Creating...
google_storage_bucket.demo-bucket: Creating...
google_bigquery_dataset.demo_dataset: Creation complete after 1s [id=projects/noted-lead-448822-q9/datasets/demo_dataset]
google_storage_bucket.demo-bucket: Creation complete after 2s [id=noted-lead-448822-q9-terra-bucket]

Apply complete! Resources: 2 added, 0 changed, 0 destroyed.
PS C:\terrademo> terraform destroy
google_storage_bucket.demo-bucket: Refreshing state... [id=noted-lead-448822-q9-terra-bucket]
google_bigquery_dataset.demo_dataset: Refreshing state... [id=projects/noted-lead-448822-q9/datasets/demo_dataset]

Terraform used the selected providers to generate the following execution plan. Resource    
actions are indicated with the following symbols:
  - destroy

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be destroyed
  - resource "google_bigquery_dataset" "demo_dataset" {
      - creation_time                   = 1737844652559 -> null
      - dataset_id                      = "demo_dataset" -> null
      - default_partition_expiration_ms = 0 -> null
      - default_table_expiration_ms     = 0 -> null
      - delete_contents_on_destroy      = false -> null
      - effective_labels                = {
          - "goog-terraform-provisioned" = "true"
        } -> null
      - etag                            = "N6ARwaZPxKdtHe1SO2hTJw==" -> null
      - id                              = "projects/noted-lead-448822-q9/datasets/demo_dataset" -> null
      - is_case_insensitive             = false -> null
      - labels                          = {} -> null
      - last_modified_time              = 1737844652559 -> null
      - location                        = "US" -> null
      - max_time_travel_hours           = "168" -> null
      - project                         = "noted-lead-448822-q9" -> null
      - resource_tags                   = {} -> null
      - self_link                       = "https://bigquery.googleapis.com/bigquery/v2/projects/noted-lead-448822-q9/datasets/demo_dataset" -> null
      - terraform_labels                = {
          - "goog-terraform-provisioned" = "true"
        } -> null
        # (4 unchanged attributes hidden)

      - access {
          - role           = "OWNER" -> null
          - user_by_email  = "terraform-runner@noted-lead-448822-q9.iam.gserviceaccount.com"
 -> null
            # (4 unchanged attributes hidden)
        }
      - access {
          - role           = "OWNER" -> null
          - special_group  = "projectOwners" -> null
            # (4 unchanged attributes hidden)
        }
      - access {
          - role           = "READER" -> null
          - special_group  = "projectReaders" -> null
            # (4 unchanged attributes hidden)
        }
      - access {
          - role           = "WRITER" -> null
          - special_group  = "projectWriters" -> null
            # (4 unchanged attributes hidden)
        }
    }

  # google_storage_bucket.demo-bucket will be destroyed
  - resource "google_storage_bucket" "demo-bucket" {
      - default_event_based_hold    = false -> null
      - effective_labels            = {
          - "goog-terraform-provisioned" = "true"
        } -> null
      - enable_object_retention     = false -> null
      - force_destroy               = true -> null
      - id                          = "noted-lead-448822-q9-terra-bucket" -> null
      - labels                      = {} -> null
      - location                    = "US" -> null
      - name                        = "noted-lead-448822-q9-terra-bucket" -> null
      - project                     = "noted-lead-448822-q9" -> null
      - project_number              = 271116629851 -> null
      - public_access_prevention    = "inherited" -> null
      - requester_pays              = false -> null
      - rpo                         = "DEFAULT" -> null
      - self_link                   = "https://www.googleapis.com/storage/v1/b/noted-lead-448822-q9-terra-bucket" -> null
      - storage_class               = "STANDARD" -> null
      - terraform_labels            = {
          - "goog-terraform-provisioned" = "true"
        } -> null
      - uniform_bucket_level_access = false -> null
      - url                         = "gs://noted-lead-448822-q9-terra-bucket" -> null      

      - hierarchical_namespace {
          - enabled = false -> null
        }

      - lifecycle_rule {
          - action {
              - type          = "AbortIncompleteMultipartUpload" -> null
                # (1 unchanged attribute hidden)
            }
          - condition {
              - age                                     = 1 -> null
              - days_since_custom_time                  = 0 -> null
              - days_since_noncurrent_time              = 0 -> null
              - matches_prefix                          = [] -> null
              - matches_storage_class                   = [] -> null
              - matches_suffix                          = [] -> null
              - num_newer_versions                      = 0 -> null
              - send_age_if_zero                        = false -> null
              - send_days_since_custom_time_if_zero     = false -> null
              - send_days_since_noncurrent_time_if_zero = false -> null
              - send_num_newer_versions_if_zero         = false -> null
              - with_state                              = "ANY" -> null
                # (3 unchanged attributes hidden)
            }
        }

      - soft_delete_policy {
          - effective_time             = "2025-01-25T22:37:33.385Z" -> null
          - retention_duration_seconds = 604800 -> null
        }
    }

Plan: 0 to add, 0 to change, 2 to destroy.

Do you really want to destroy all resources?
  Terraform will destroy all your managed infrastructure, as shown above.
  There is no undo. Only 'yes' will be accepted to confirm.

  Enter a value: yes

google_storage_bucket.demo-bucket: Destroying... [id=noted-lead-448822-q9-terra-bucket]
google_bigquery_dataset.demo_dataset: Destroying... [id=projects/noted-lead-448822-q9/datasets/demo_dataset]
google_bigquery_dataset.demo_dataset: Destruction complete after 0s
google_storage_bucket.demo-bucket: Destruction complete after 1s

Destroy complete! Resources: 2 destroyed.
```

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw1
