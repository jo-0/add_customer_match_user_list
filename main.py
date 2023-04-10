import csv
import functions_framework
import hashlib
import os

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.cloud import storage


os.environ["GOOGLE_ADS_CONFIGURATION_FILE_PATH"] = "google-ads.yaml"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "silken-tenure-383314-4699d25cba08.json"


@functions_framework.http
def add_customer_match_user_list(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    if request.method == "POST":
        request_data = request.get_json(force=True)

        if request_data:
            if any([
                ("bucket_name" not in request_data),
                ("blob_name" not in request_data),
                ("customer_id" not in request_data)
            ]):
                return "Not enough data", 400

            if ("user_list_id" in request_data):
                user_list_id = request_data["user_list_id"]
            else:
                user_list_id = ""

            bucket_name = request_data["bucket_name"]
            blob_name = request_data["blob_name"]
            customer_id = request_data["customer_id"]

            ga_client = GoogleAdsClient.load_from_storage()

            raw_records = get_file_from_gcs(
                blob_name=blob_name,
                file_path="/tmp/file.csv",
                bucket_name=bucket_name
            )

            try:
                main(
                    ga_client=ga_client,
                    customer_id=customer_id,
                    run_job=True,
                    user_list_id=user_list_id,
                    records=raw_records
                )
            except GoogleAdsException as ex:
                print(
                    f"Request with ID '{ex.request_id}' failed with status "
                    f"'{ex.error.code().name}' and includes the following errors:"
                )
                for error in ex.failure.errors:
                    print(f"\tError with message '{error.message}'.")
                    if error.location:
                        for field_path_element in error.location.field_path_elements:
                            print(f"\t\tOn field: {field_path_element.field_name}")
                return f"{ex.error.code().name}", 500

        else:
            return "Bad request", 400


def main(
    ga_client,
    customer_id,
    run_job,
    user_list_id,
    records
):
    googleads_service = ga_client.get_service("GoogleAdsService")

    if user_list_id:
        # Override the user_list if it already exists.
        replace = True
        # Uses the specified Customer Match user list.
        user_list_resource_name = googleads_service.user_list_path(
            customer_id, user_list_id
        )
    else:
        # Creates a Customer Match user list.
        user_list_resource_name = create_customer_match_user_list(
            ga_client, customer_id
        )
        replace = False

    add_users_to_customer_match_user_list(
        ga_client=ga_client,
        customer_id=customer_id,
        user_list_resource_name=user_list_resource_name,
        run_job=run_job,
        replace=replace,
        records=records
    )


def create_customer_match_user_list(client, customer_id, list_name="Customer Match list"):
    """Creates a Customer Match user list.
    Args:
        client: The Google Ads client.
        customer_id: The ID for the customer that owns the user list.
    Returns:
        The string resource name of the newly created user list.
    """
    # Creates the UserListService client.
    user_list_service_client = client.get_service("UserListService")

    # Creates the user list operation.
    user_list_operation = client.get_type("UserListOperation")

    # Creates the new user list.
    user_list = user_list_operation.create
    user_list.name = list_name

    # Sets the upload key type to indicate the type of identifier that is used
    # to add users to the list. This field is immutable and required for a
    # CREATE operation.
    user_list.crm_based_user_list.upload_key_type = (
        client.enums.CustomerMatchUploadKeyTypeEnum.CONTACT_INFO
    )
    # Customer Match user lists can set an unlimited membership life span;
    # to do so, use the special life span value 10000. Otherwise, membership
    # life span must be between 0 and 540 days inclusive. See:
    # https://developers.devsite.corp.google.com/google-ads/api/reference/rpc/latest/UserList#membership_life_span
    # Sets the membership life span to 30 days.
    user_list.membership_life_span = 30

    response = user_list_service_client.mutate_user_lists(
        customer_id=customer_id, operations=[user_list_operation]
    )
    user_list_resource_name = response.results[0].resource_name
    print(
        f"User list with resource name '{user_list_resource_name}' was created."
    )

    return user_list_resource_name


def add_users_to_customer_match_user_list(
    ga_client,
    customer_id,
    user_list_resource_name,
    run_job,
    replace,
    records,
    offline_user_data_job_id=None,
):
    """Uses Customer Match to create and add users to a new user list.
    Args:
        client: The Google Ads client.
        customer_id: The ID for the customer that owns the user list.
        user_list_resource_name: The resource name of the user list to which to
            add users.
        run_job: If true, runs the OfflineUserDataJob after adding operations.
            Otherwise, only adds operations to the job.
        offline_user_data_job_id: ID of an existing OfflineUserDataJob in the
            PENDING state. If None, a new job is created.
    """
    # Creates the OfflineUserDataJobService client.
    offline_user_data_job_service_client = ga_client.get_service(
        "OfflineUserDataJobService"
    )

    if offline_user_data_job_id:
        # Reuses the specified offline user data job.
        offline_user_data_job_resource_name = offline_user_data_job_service_client.offline_user_data_job_path(
            customer_id, offline_user_data_job_id
        )
    else:
        # Creates a new offline user data job.
        offline_user_data_job = ga_client.get_type("OfflineUserDataJob")
        offline_user_data_job.type_ = (
            ga_client.enums.OfflineUserDataJobTypeEnum.CUSTOMER_MATCH_USER_LIST
        )
        offline_user_data_job.customer_match_user_list_metadata.user_list = (
            user_list_resource_name
        )
        # Issues a request to create an offline user data job.
        create_offline_user_data_job_response = offline_user_data_job_service_client.create_offline_user_data_job(
            customer_id=customer_id, job=offline_user_data_job
        )
        offline_user_data_job_resource_name = (
            create_offline_user_data_job_response.resource_name
        )
        print(
            "Created an offline user data job with resource name: "
            f"'{offline_user_data_job_resource_name}'."
        )

    # Issues a request to add the operations to the offline user data job.

    # Best Practice: This example only adds a few operations, so it only sends
    # one AddOfflineUserDataJobOperations request. If your application is adding
    # a large number of operations, split the operations into batches and send
    # multiple AddOfflineUserDataJobOperations requests for the SAME job. See
    # https://developers.google.com/google-ads/api/docs/remarketing/audience-types/customer-match#customer_match_considerations
    # and https://developers.google.com/google-ads/api/docs/best-practices/quotas#user_data
    # for more information on the per-request limits.
    operations = []
    if replace:
        operation = ga_client.get_type("OfflineUserDataJobOperation")
        operation.remove_all = True
        operations.append(operation)

    operations.extend(build_offline_user_data_job_operations(ga_client, records))

    request = ga_client.get_type("AddOfflineUserDataJobOperationsRequest")
    request.resource_name = offline_user_data_job_resource_name
    request.operations = operations
    request.enable_partial_failure = True

    # Issues a request to add the operations to the offline user data job.
    response = offline_user_data_job_service_client.add_offline_user_data_job_operations(
        request=request
    )

    # Prints the status message if any partial failure error is returned.
    # Note: the details of each partial failure error are not printed here.
    # Refer to the error_handling/handle_partial_failure.py example to learn
    # more.
    # Extracts the partial failure from the response status.
    partial_failure = getattr(response, "partial_failure_error", None)
    if getattr(partial_failure, "code", None) != 0:
        error_details = getattr(partial_failure, "details", [])
        for error_detail in error_details:
            failure_message = ga_client.get_type("GoogleAdsFailure")
            # Retrieve the class definition of the GoogleAdsFailure instance
            # in order to use the "deserialize" class method to parse the
            # error_detail string into a protobuf message object.
            failure_object = type(failure_message).deserialize(
                error_detail.value
            )

            for error in failure_object.errors:
                print(
                    "A partial failure at index "
                    f"{error.location.field_path_elements[0].index} occurred.\n"
                    f"Error message: {error.message}\n"
                    f"Error code: {error.error_code}"
                )

    print("The operations are added to the offline user data job.")

    if not run_job:
        print(
            "Not running offline user data job "
            f"'{offline_user_data_job_resource_name}', as requested."
        )
        return

    # Issues a request to run the offline user data job for executing all
    # added operations.
    offline_user_data_job_service_client.run_offline_user_data_job(
        resource_name=offline_user_data_job_resource_name
    )

    # Retrieves and displays the job status.
    check_job_status(ga_client, customer_id, offline_user_data_job_resource_name)


def check_job_status(ga_client, customer_id, offline_user_data_job_resource_name):
    """Retrieves, checks, and prints the status of the offline user data job.
    If the job is completed successfully, information about the user list is
    printed. Otherwise, a GAQL query will be printed, which can be used to
    check the job status at a later date.
    Offline user data jobs may take 6 hours or more to complete, so checking the
    status periodically, instead of waiting, can be more efficient.
    Args:
        client: The Google Ads client.
        customer_id: The ID for the customer that owns the user list.
        offline_user_data_job_resource_name: The resource name of the offline
            user data job to get the status of.
    """
    query = f"""
        SELECT
          offline_user_data_job.resource_name,
          offline_user_data_job.id,
          offline_user_data_job.status,
          offline_user_data_job.type,
          offline_user_data_job.failure_reason,
          offline_user_data_job.customer_match_user_list_metadata.user_list
        FROM offline_user_data_job
        WHERE offline_user_data_job.resource_name =
          '{offline_user_data_job_resource_name}'
        LIMIT 1"""

    # Issues a search request using streaming.
    google_ads_service = ga_client.get_service("GoogleAdsService")
    results = google_ads_service.search(customer_id=customer_id, query=query)
    offline_user_data_job = next(iter(results)).offline_user_data_job
    status_name = offline_user_data_job.status.name
    user_list_resource_name = (
        offline_user_data_job.customer_match_user_list_metadata.user_list
    )

    print(
        f"Offline user data job ID '{offline_user_data_job.id}' with type "
        f"'{offline_user_data_job.type_.name}' has status: {status_name}"
    )

    if status_name == "SUCCESS":
        print_customer_match_user_list_info(
            ga_client, customer_id, user_list_resource_name
        )
    elif status_name == "FAILED":
        print(f"\tFailure Reason: {offline_user_data_job.failure_reason}")
    elif status_name in ("PENDING", "RUNNING"):
        print(
            "To check the status of the job periodically, use the following "
            f"GAQL query with GoogleAdsService.Search: {query}"
        )


def print_customer_match_user_list_info(
    ga_client, customer_id, user_list_resource_name
):
    """Prints information about the Customer Match user list.
    Args:
        client: The Google Ads client.
        customer_id: The ID for the customer that owns the user list.
        user_list_resource_name: The resource name of the user list to which to
            add users.
    """
    googleads_service_client = ga_client.get_service("GoogleAdsService")

    # Creates a query that retrieves the user list.
    query = f"""
        SELECT
          user_list.size_for_display,
          user_list.size_for_search
        FROM user_list
        WHERE user_list.resource_name = '{user_list_resource_name}'"""

    # Issues a search request.
    search_results = googleads_service_client.search(
        customer_id=customer_id, query=query
    )

    # Prints out some information about the user list.
    user_list = next(iter(search_results)).user_list
    print(
        "The estimated number of users that the user list "
        f"'{user_list.resource_name}' has is "
        f"{user_list.size_for_display} for Display and "
        f"{user_list.size_for_search} for Search."
    )
    print(
        "Reminder: It may take several hours for the user list to be "
        "populated. Estimates of size zero are possible."
    )


def build_offline_user_data_job_operations(ga_client, raw_records):
    """Creates a raw input list of unhashed user information.

    Each element of the list represents a single user and is a dict containing a
    separate entry for the keys "email", "phone", "first_name", "last_name",
    "country_code", and "postal_code".

    Args:
        ga_client: The Google Ads client.

    Returns:
        A list containing the operations.
    """

    operations = []
    # Iterates over the raw input list and creates a UserData object for each
    # record.
    for record in raw_records:
        # Creates a UserData object that represents a member of the user list.
        user_data = ga_client.get_type("UserData")

        # Checks if the record has email, phone, or address information, and
        # adds a SEPARATE UserIdentifier object for each one found. For example,
        # a record with an email address and a phone number will result in a
        # UserData with two UserIdentifiers.

        # IMPORTANT: Since the identifier attribute of UserIdentifier
        # (https://developers.google.com/google-ads/api/reference/rpc/latest/UserIdentifier)
        # is a oneof
        # (https://protobuf.dev/programming-guides/proto3/#oneof-features), you
        # must set only ONE of hashed_email, hashed_phone_number, mobile_id,
        # third_party_user_id, or address-info. Setting more than one of these
        # attributes on the same UserIdentifier will clear all the other members
        # of the oneof. For example, the following code is INCORRECT and will
        # result in a UserIdentifier with ONLY a hashed_phone_number:

        # incorrect_user_identifier = client.get_type("UserIdentifier")
        # incorrect_user_identifier.hashed_email = "..."
        # incorrect_user_identifier.hashed_phone_number = "..."

        # The separate 'if' statements below demonstrate the correct approach
        # for creating a UserData object for a member with multiple
        # UserIdentifiers.

        # Checks if the record has an email address, and if so, adds a
        # UserIdentifier for it.
        if "Email" in record:
            user_identifier = ga_client.get_type("UserIdentifier")
            user_identifier.hashed_email = normalize_and_hash(
                record["Email"], True
            )
            # Adds the hashed email identifier to the UserData object's list.
            user_data.user_identifiers.append(user_identifier)

        # Checks if the record has a phone number, and if so, adds a
        # UserIdentifier for it.
        if "Phone" in record:
            user_identifier = ga_client.get_type("UserIdentifier")
            user_identifier.hashed_phone_number = normalize_and_hash(
                record["Phone"], True
            )
            # Adds the hashed phone number identifier to the UserData object's
            # list.
            user_data.user_identifiers.append(user_identifier)

        # Checks if the record has all the required mailing address elements,
        # and if so, adds a UserIdentifier for the mailing address.
        if "First name" in record:
            required_keys = ("Last name", "Country", "Zip")
            # Checks if the record contains all the other required elements of
            # a mailing address.
            if not all(key in record for key in required_keys):
                # Determines which required elements are missing from the
                # record.
                missing_keys = record.keys() - required_keys
                print(
                    "Skipping addition of mailing address information "
                    "because the following required keys are missing: "
                    f"{missing_keys}"
                )
            else:
                user_identifier = ga_client.get_type("UserIdentifier")
                address_info = user_identifier.address_info
                address_info.hashed_first_name = normalize_and_hash(
                    record["First name"], False
                )
                address_info.hashed_last_name = normalize_and_hash(
                    record["Last name"], False
                )
                address_info.country_code = record["Country"]
                address_info.postal_code = record["Zip"]
                user_data.user_identifiers.append(user_identifier)

        # If the user_identifiers repeated field is not empty, create a new
        # OfflineUserDataJobOperation and add the UserData to it.
        if user_data.user_identifiers:
            operation = ga_client.get_type("OfflineUserDataJobOperation")
            operation.create = user_data
            operations.append(operation)

    return operations


def normalize_and_hash(s, remove_all_whitespace):
    """Normalizes and hashes a string with SHA-256.
    Args:
        s: The string to perform this operation on.
        remove_all_whitespace: If true, removes leading, trailing, and
            intermediate spaces from the string before hashing. If false, only
            removes leading and trailing spaces from the string before hashing.
    Returns:
        A normalized (lowercase, remove whitespace) and SHA-256 hashed string.
    """
    # Normalizes by first converting all characters to lowercase, then trimming
    # spaces.
    if remove_all_whitespace:
        # Removes leading, trailing, and intermediate whitespace.
        s = "".join(s.split())
    else:
        # Removes only leading and trailing spaces.
        s = s.strip().lower()

    # Hashes the normalized string using the hashing algorithm.
    return hashlib.sha256(s.encode()).hexdigest()


def get_file_from_gcs(blob_name, file_path, bucket_name):
    try:
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        blob.download_to_filename(filename=file_path)
        with open(file_path) as file:
            csv_reader = csv.DictReader(file)
            records = list(csv_reader)

        return records

    except Exception as e:
        print("Failed to get file: ", e)
