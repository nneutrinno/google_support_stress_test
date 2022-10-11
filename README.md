# STRESS TEST

## Requirements

## An '.env' file in the same folder
The .env must contain the following fields.

#### APP_ENV
Datastore namespace.
#### GCP_PROJECT_ID
Datastore project id.
#### KIND
Datastore entity kind.
#### GCP_CREDENTIALS_PATH
The json's path. It must follow the interface bellow.

```ts
interface JSON {
    type: string;
    project_id: string;
    private_key_id: string;
    private_key: string;
    client_email: string;
    client_id: string;
    auth_uri: string;
    token_uri: string;
    auth_provider_x509_cert_url: string;
    client_x509_cert_url: string;
}
```


