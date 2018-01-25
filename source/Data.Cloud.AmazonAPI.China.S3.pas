unit Data.Cloud.AmazonAPI.China.S3;
//AWS China API S3 接口
//参考  Data.Cloud.AmazonAPI
//有兴趣可以自行测试
//Bug:371889755@qq.com

interface

uses
  System.Classes,
  System.JSON,
  System.SysUtils,
  System.Generics.Collections,
  Data.Cloud.CloudAPI, Data.Cloud.AmazonAPI.LifeCycle,
  Xml.XMLIntf,
  Data.Cloud.AmazonAPI,
  Data.Cloud.AmazonAPI.China;

type

  TAmazonChinaStorageService = class(TAmazonChinaBaseService)
  private
    function InitHeaders(const BucketName: string = ''): TStringList;
    procedure AddAndValidateHeaders(const defaultHeaders, customHeaders: TStrings);
    function PopulateResultItem(ObjectNode: IXMLNode; out ResultItem: TAmazonObjectResult): Boolean;
    procedure PopulateGrants(GrantsNode: IXMLNode; Grants: TList<TAmazonGrant>);
    function GetBucketInternal(const XML: string; ResponseInfo: TCloudResponseInfo): TAmazonBucketResult;
    function GetBucketXMLInternal(const BucketName: string; OptionalParams: TStrings; VersionInfo: Boolean;
                                  ResponseInfo: TCloudResponseInfo; BucketRegion: TAmazonChinaRegion = amzrNotSpecified): string;
    function GetNotificationXML(Events: TList<TAmazonNotificationEvent>): string;
    function DeleteObjectInternal(const BucketName, ObjectName, VersionId: string;
                                  ResponseInfo: TCloudResponseInfo = nil): Boolean;
    function GetObjectInternal(const BucketName, ObjectName, VersionId: string;
                               OptionalParams: TAmazonGetObjectOptionals;
                               ResponseInfo: TCloudResponseInfo; ObjectStream: TStream): Boolean;
    function GetVirtualHostFromRegion(const bucketname: string; location: TAmazonChinaRegion): string;
  protected
    /// <summary>The lazy-loaded list of required header names.</summary>
    FRequiredHeaderNames: TStrings;

    /// <summary>URL Encodes the param name and value.</summary>
    /// <remarks>Skips encoding if not for URL.</remarks>
    /// <param name="ForURL">True if the parameter is for a URL, false if it is for a signature.</param>
    /// <param name="ParamName">Name of the parameter</param>
    /// <param name="ParamValue">Value of the parameter</param>
    procedure URLEncodeQueryParams(const ForURL: Boolean; var ParamName, ParamValue: string); override;
    /// <summary>Returns the authentication instance to use for the given connection info.</summary>
    /// <returns>The authentication instance to use for the given connection info.</returns>
    function CreateAuthInstance(const ConnectionInfo: TAmazonConnectionInfo): TCloudAuthentication; override;
    /// <summary>Builds the StringToSign value, based on the given information.</summary>
    /// <param name="HTTPVerb">The HTTP verb (eg: GET, POST) of the request type.</param>
    /// <param name="Headers">The list of headers in the request, or nil</param>
    /// <param name="QueryParameters">The list of query parameters for the request, or nil</param>
    /// <param name="QueryPrefix">The string to prefix the query parameter string with.</param>
    /// <param name="URL">The URL of the request.</param>
    /// <returns>The StringToSign, which will be encoded and used for authentication.</returns>
    function BuildStringToSign(const HTTPVerb: string; Headers, QueryParameters: TStringList;
                               const QueryPrefix, URL: string): string; override;
    /// <summary>Handles the StringToSign after it is created.</summary>
    /// <remarks>This modifies the provided URL or the content stream, adding a 'Signature' query parameter.
    /// </remarks>
    /// <param name="HTTPVerb">The HTTP verb (eg: GET, POST) of the request.</param>
    /// <param name="Headers">The header name/value pairs</param>
    /// <param name="QueryParameters">The query parameter name/value pairs</param>
    /// <param name="StringToSign">The StringToSign for the request</param>
    /// <param name="URL">The URL of the request</param>
    /// <param name="Request">The request object</param>
    /// <param name="Content">The content stream of the request.</param>
    procedure PrepareRequestSignature(const HTTPVerb: string;
                                      const Headers, QueryParameters: TStringList;
                                      const StringToSign: string;
                                      var URL: string; Request: TCloudHTTP; var Content: TStream); override;
    /// <summary>Builds the StringToSign value's header part</summary>
    /// <remarks>This will include both the required and optional headers, and end with a newline character.
    /// </remarks>
    /// <param name="Headers">The list of headers in the request, or nil</param>
    /// <returns>The headers portion of the StringToSign</returns>
    function BuildStringToSignHeaders(Headers: TStringList): string; override;
    /// <summary>Returns the list of required header names</summary>
    /// <remarks>Implementation of abstract declaration in parent class.
    ///    Lazy-loads and returns FRequiredHeaderNames. Sets InstanceOwner to false,
    ///    since this class instance will manage the memory for the object.
    /// </remarks>
    /// <param name="InstanceOwner">Returns false, specifying the caller doesn't own the list.</param>
    /// <returns>The list of required hear names. No values.</returns>
    function GetRequiredHeaderNames(out InstanceOwner: Boolean): TStrings; override;
    /// <summary>Returns the header name prefix for Amazon services, for headers to be included
    ///     as name/value pairs in the StringToSign in authentication.
    /// </summary>
    /// <returns>The header prefix (x-amz-)</returns>
    function GetCanonicalizedHeaderPrefix: string; override;
    /// <summary>Returns the current time, formatted properly as a string.</summary>
    /// <returns>The current time, formatted properly as a string</returns>
    function CurrentTime: string; override;
    /// <summary>Populates the given ResponseInfo with error information, if any.</summary>
    /// <remarks>If the given ResponseInfo is non-null, and the ResultString holds an XML error message,
    ///          then this procedure will parse the error XML and populate the ResponseInfo's message
    ///          with the error message.
    /// </remarks>
    /// <param name="ResponseInfo">The optional response info to populate, or nil</param>
    /// <param name="ResultString">The request's response string to parse for an error message.</param>
    procedure ParseResponseError(const ResponseInfo: TCloudResponseInfo; const ResultString: string);
    /// <summary>Sorts the given list of Headers.</summary>
    /// <remarks>Extend this if you wish to modify how the list is sorted.</remarks>
    /// <param name="Headers">List of headers to sort.</param>
    procedure SortHeaders(const Headers: TStringList); override;
    function BuildQueryParameterString(const QueryPrefix: string; QueryParameters: TStringList;
                                       DoSort: Boolean = False; ForURL: Boolean = True): string; override;
  public
    /// <summary>Creates a new instance of TAmazonChinaStorageService</summary>
    /// <remarks>This class does not own the ConnectionInfo instance.</remarks>
    // / <param name="ConnectionInfo">The Amazon service connection info</param>
    constructor Create(const ConnectionInfo: TAmazonChinaConnectionInfo);
    /// <summary>Frees the required headers list and destroys the instance</summary>
    destructor Destroy; override;

    /// <summary>Lists the buckets owned by the current AWS account.</summary>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of all the account's buckets.</returns>
    function ListBucketsXML(const ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Lists the buckets owned by the current AWS account.</summary>
    /// <remarks>The list returned are key/value pairs, where the keys are the bucket names,
    ///          and the values are the creation dates for each bucket. The date is in the
    ///          format: 2011-01-21T10:30:57.000Z ('yyyy-mm-ddThh:nn:ss.zzzZ')
    ///          Note that when parsing the date you may need to escape the 'T' and 'Z'.
    /// </remarks>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The list of all the account's buckets.</returns>
    function ListBuckets(const ResponseInfo: TCloudResponseInfo = nil): TStrings;
    /// <summary>Creates a new bucket with the given name on the S3 service.</summary>
    /// <remarks>Bucket names must be unique to the S3 service. That means if someone else has already used a
    ///          given bucket name with their account, you cannot create a bucket with the same name.
    ///
    ///          Bucket names have some restrictions:
    ///           They must start with a number or letter.
    ///           They can contain periods, underscores and dashes, numbers and lowercase letters.
    ///           They must be between 3 and 255 characters (although shouldn't be longer than 63 characters.)
    ///           They must not be formatted like an IP address (e.g., 192.168.0.1)
    ///
    ///          Furthermore, if you specify a Region when creating the bucket, you must also follow these rules:
    ///           The name can't contain underscores.
    ///           The name must be between 3 and 63 characters long.
    ///           The name can't end in a dash
    ///           The name cannot contain two adjacent periods
    ///           The name cannot contain a dash next to a period. (e.g., 'my.-bucket')
    ///
    ///          You can choose to set the Bucket's access control list and/or the region with this call.
    ///          You can choose a Region to reduce costs or to optimize latency.
    ///          For example, if you are in Europe, you will probably want to create buckets in the EU Region.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to create</param>
    /// <param name="BucketACL">The ACL value to use in the bucket creation</param>
    /// <param name="BucketRegion">The region to create the bucket in</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the creation was successful, false otherwise.</returns>
    function CreateBucket(const BucketName: string; BucketACL: TAmazonACLType = amzbaPrivate;
                          BucketRegion: TAmazonChinaRegion = amzrNotSpecified;
                          ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Deletes the given Bucket.</summary>
    /// <param name="BucketName">The name of the bucket to delete</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <param name="BucketRegion">The region of the bucket to delete</param>
    /// <returns>True if the deletion was successful, false otherwise.</returns>
    function DeleteBucket(const BucketName: string;
                          ResponseInfo: TCloudResponseInfo = nil;
                          BucketRegion: TAmazonChinaRegion = amzrNotSpecified): Boolean;
    /// <summary>Deletes the policy on the given Bucket.</summary>
    /// <param name="BucketName">The name of the bucket to delete the policy for</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <param name="BucketRegion">The region of the bucket to delete</param>
    /// <returns>True if the deletion was successful, false otherwise.</returns>
    function DeleteBucketPolicy(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil;
                               BucketRegion: TAmazonChinaRegion = amzrNotSpecified): Boolean;
    /// <summary>Returns some or all of the objects of a given bucket.</summary>
    /// <remarks>The optional parameters allow for filtering the results as well as creating a virtual
    ///          directory structure. The supported parameters include:
    ///           delimiter: commonly set to '/' this sets the character to denote a directory
    ///           prefix: Limits the response to object names that begin with the specified prefix
    ///           marker: continuation token, specifying the name of the object to begin population at
    ///           max-keys: (integer) the max number of objects to return.
    ///
    ///          If you want to traverse the objects in a directory structure, then sent the delimiter to
    ///          a character to be used as a path separator, such as a slash character ('/'). The results
    ///          you get back will contain any objects under the 'root directory' and will also contain a
    ///          list of 'prefixes' which are the names of the subdirectories. To traverse the subdirectories,
    ///          set the absolute path of the subdirectory (the prefix value) as the 'prefix' in the next call
    ///          leaving the 'delimiter' as a slash.
    ///
    ///          When more than the maximum number of objects to return exists, 'Truncated' will be set to true.
    ///          To get more objects, use the name of the last object you got as the 'marker' value. That object
    ///          will be populated again in the next call, but none that came before it will.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the objects for</param>
    /// <param name="OptionalParams">Optional parameters for filtering the results. See remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <param name="BucketRegion">The region of the bucket</param>
    /// <returns>The XML representation of the bucket's objects and additional information.</returns>
    function GetBucketXML(const BucketName: string; OptionalParams: TStrings;
                          ResponseInfo: TCloudResponseInfo = nil; BucketRegion: TAmazonChinaRegion = amzrNotSpecified): string;
    /// <summary>Returns some or all of the objects of a given bucket.</summary>
    /// <remarks>For information on the Optional parameters, see remarks on GetBucketXML.
    ///
    ///          When more than the maximum number of objects to return exists, 'Truncated' will be set to true.
    ///          To get more objects, use the name of the last object you got as the 'marker' value. That object
    ///          will be populated again in the next call, but none that came before it will. For convenience,
    ///          this marker value will be placed in the ResponseInfo header 'marker' is a ResponseInfo instance
    ///          is provided and the list of bucket objects is truncated.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the objects for</param>
    /// <param name="OptionalParams">Optional parameters for filtering the results. See remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <param name="BucketRegion">The region of the bucket</param>
    /// <returns>The bucket's objects and additional information.</returns>
    function GetBucket(const BucketName: string; OptionalParams: TStrings;
                       ResponseInfo: TCloudResponseInfo = nil; BucketRegion: TAmazonChinaRegion = amzrNotSpecified): TAmazonBucketResult;
    /// <summary>Returns the given bucket's ACL</summary>
    /// <remarks>To get the ACL of the bucket, you must have READ_ACP access to the bucket.
    ///          If READ_ACP permission is set for anonymous users, you can return the bucket's ACL
    ///          without using an authorization header.
    ///
    ///          The possible permissions are:
    ///            FULL_CONTROL
    ///            WRITE
    ///            WRITE_ACP - allow writing the ACL of the bucket
    ///            READ
    ///            READ_ACP - allow reading the ACL of the bucket
    ///
    ///         Users assigned multiple permissions will appear multiple times in the Grant list.
    ///
    ///         All users are granted a permission when the Grantee has a URI of:
    ///          http://acs.amazonaws.com/groups/global/AllUsers.
    ///         All authenticated users are granted a permission when the Grantee has a URI of:
    ///          http://acs.amazonaws.com/groups/global/AuthenticatedUsers.
    ///         The Log delivery group is granted permission when the Grantee has a URI of:
    ///          http://acs.amazonaws.com/groups/global/LogDelivery
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the access control list for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the bucket's ACL</returns>
    function GetBucketACLXML(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns the given bucket's ACL</summary>
    /// <remarks>Users assigned multiple permissions will appear multiple times in the Grant list.
    ///          For more information, see remarks on GetBucketACLXML;
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the access control list for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The bucket's ACL</returns>
    function GetBucketACL(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): TList<TAmazonGrant>;
    /// <summary>Returns the given bucket's policies</summary>
    /// <remarks>This returns a string which, if the request is successful, is a JSON representation of
    ///          the policies. See the Amazon S3 documentation for more information on the format.
    ///
    ///          If no policy exists for the given bucket, then the response will be in XML, and will be an error
    ///          message explaining that that the bucket policy doesn't exist. The response code will be 404.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the policies for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The JSON String representation of the bucket's policies</returns>
    function GetBucketPolicyJSON(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns the given bucket's policies</summary>
    /// <remarks>If the request is successful this returns a JSON representation of
    ///          the policies. See the Amazon S3 documentation for more information on the format.
    ///
    ///          If no policy exists for the given bucket, then the response will be nil and
    ///          the response code will be 404.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the policies for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The JSON String representation of the bucket's policies</returns>
    function GetBucketPolicy(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): TJSONObject;
    /// <summary>Returns the given bucket's location</summary>
    /// <param name="BucketName">The name of the bucket to get the location for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the bucket's location</returns>
    function GetBucketLocationXML(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns the given bucket's location</summary>
    /// <remarks>Returns amzrNotSpecified if the request fails.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the location for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The bucket's region, or empty string for US Classic</returns>
    function GetBucketLocation(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): TAmazonChinaRegion;
    /// <summary>Returns the given bucket's logging information</summary>
    /// <remarks>This returns the logging status for the bucket as well as the permissions users have
    ///          to view and modify the status.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the logging information for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the bucket's logging information</returns>
    function GetBucketLoggingXML(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns the given bucket's logging information</summary>
    /// <remarks>This returns the logging status for the bucket as well as the permissions users have
    ///          to view and modify the status.
    ///
    ///          Returns nil if the request fails. Returns a TAmazonBucketLoggingInfo with IsLoggingEnabled
    ///          returning False if logging is disabled on the bucket.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the logging information for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The bucket's logging information</returns>
    function GetBucketLogging(const BucketName: string;
                              ResponseInfo: TCloudResponseInfo = nil): TAmazonBucketLoggingInfo;
    /// <summary>Returns the given bucket's notification configuration</summary>
    /// <remarks>Currently, the s3:ReducedRedundancyLostObject event is the only event supported by Amazon S3.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the notification configuration for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <param name="BucketRegion">The optional region for the notification config</param>
    /// <returns>The XML representation of the bucket's notification configuration</returns>
    function GetBucketNotificationXML(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil;
                                      BucketRegion: TAmazonChinaRegion = amzrNotSpecified): string;
    /// <summary>Returns the given bucket's notification configuration</summary>
    /// <remarks>If the request fails nil will be returned.
    ///          Currently, the s3:ReducedRedundancyLostObject event is the only event supported by Amazon S3.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the notification configuration for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <param name="BucketRegion">The optional region for the notification config</param>
    /// <returns>The bucket's notification configuration</returns>
    function GetBucketNotification(const BucketName: string;
                                   ResponseInfo: TCloudResponseInfo = nil;
                                   BucketRegion: TAmazonChinaRegion = amzrNotSpecified): TList<TAmazonNotificationEvent>;
    /// <summary>Returns some or all of the objects of a given bucket, returning all versions of each object.</summary>
    /// <remarks>The optional parameters include all optional parameters supported by the GetBucket command,
    ///          except that 'marker' should be called 'key-marker'. Also, 'version-id-marker' is also supported,
    ///          which can be used as a continuation token for a specific file version to continue from.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the objects/versions for</param>
    /// <param name="OptionalParams">Optional parameters for filtering the results. See remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the bucket's objects/versions and additional information.</returns>
    function GetBucketObjectVersionsXML(const BucketName: string; OptionalParams: TStrings;
                                        ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns some or all of the objects of a given bucket.</summary>
    /// <remarks>See remarks on GetBucketObjectVersionsXML for more information.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the objects/versions for</param>
    /// <param name="OptionalParams">Optional parameters for filtering the results. See remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The bucket's objects/versions and additional information.</returns>
    function GetBucketObjectVersions(const BucketName: string; OptionalParams: TStrings;
                                     ResponseInfo: TCloudResponseInfo = nil): TAmazonBucketResult;
    /// <summary>Returns the user who pays for the given bucket's access.</summary>
    /// <remarks>The options are either the current requester (requires AWS authentication) or the bucket owner.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the payer information for</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the bucket's payer information.</returns>
    function GetRequestPaymentXML(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns the user who pays for the given bucket's access.</summary>
    /// <remarks>The options are either the current requester (requires AWS authentication) or the bucket owner.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the payer information for</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The bucket's payer, or ampUnknown if the request fails.</returns>
    function GetRequestPayment(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): TAmazonPayer;
    /// <summary>Returns the versioning configuration for the specified bucket.</summary>
    /// <remarks>The status is 'Enabled' if the given bucket has versioning turned on. Otherwise,
    ///          it is 'Suspended' if versioning has ever been turned on or not specified at all
    ///          if versioning has never been enabled for the specified bucket.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the versioning state for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the versioning configuration.</returns>
    function GetBucketVersioningXML(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns the state of versioning for the specified bucket.</summary>
    /// <remarks>Returns true if versioning is enabled for the given bucket. If false is returned
    ///          than either versioning is suspended, or has never been enabled.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the versioning state for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if versioning is enabled, false otherwise.</returns>
    function GetBucketVersioning(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Returns the state of MFA (Multi-Factor-Authentication) Delete for the specified bucket.</summary>
    /// <remarks>Returns true if MFA Delete is enabled for the given bucket, false otherwise.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the MFA Delete state for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if MFA is enabled, false otherwise or if the request fails.</returns>
    function GetBucketMFADelete(const BucketName: string; ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Returns the lifecycle configuration information set on the bucket.</summary>
    /// <remarks>
    ///   To use this operation, you must have permission to perform the s3:GetLifecycleConfiguration action.
    ///   The bucket owner has this permission, by default. The bucket owner can grant this permission to others.
    /// </remarks>
    /// <param name="ABucketName">The name of the bucket to get the lifecycle.</param>
    /// <param name="AResponseInfo">The optional class for storing response info into.</param>
    /// <returns>The XML representation of the stored lifecycle.</returns>
    function GetBucketLifecycleXML(const ABucketName: string; const AResponseInfo: TCloudResponseInfo): string;
    /// <summary>Returns a list of the current active Multipart Uploads.</summary>
    /// <remarks>This lists in-progress multipart uploads and all of their parts.
    ///      The call returns, at most, 1000 result items. The supported optional parameters include:
    ///       delimiter: used to group keys or traverse a virtual directory structure (e.g., '/').
    ///       prefix: Limits the response to object names that begin with the specified prefix
    ///       max-uploads: integer between 1 and 1000. Maximum number of items to return.
    ///       key-marker: Says which file to begin population from. If upload-id-marker isn't specified then
    ///                   population begins from the next file after this file name. Otherwise, population
    ///                   begins at the next upload part if one exists, or the next file.
    ///       upload-id-marker: specifies the multipart upload item to continue population from.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the list of active multipart uploads for.</param>
    /// <param name="OptionalParams">The optional request parameters. See Remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the active multipart upload list.</returns>
    function ListMultipartUploadsXML(const BucketName: string;
                                     const OptionalParams: TStrings;
                                     ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns a list of the current active Multipart Uploads.</summary>
    /// <remarks>This lists in-progress multipart uploads and all of their parts.
    ///          For information on the OptionalParams, see remarks on ListMultipartUploadsXML.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to get the list of active multipart uploads for.</param>
    /// <param name="OptionalParams">The optional request parameters. See Remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The ative multipart uploads result, or nil if the request fails.</returns>
    function ListMultipartUploads(const BucketName: string;
                                  const OptionalParams: TStrings;
                                  ResponseInfo: TCloudResponseInfo = nil): TAmazonMultipartUploadsResult;
    /// <summary>Sets the ACL for the given bucket.</summary>
    /// <remarks>The given ACP holds the owner information as well as the ACL.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to set the ACL for.</param>
    /// <param name="ACP">The access control policy containing owner info and the ACL.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetBucketACL(const BucketName: string; ACP: TAmazonAccessControlPolicy;
                          ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Sets the Policy for the given bucket.</summary>
    /// <remarks>For information on the Policy JSON format, see the Amazon S3 documentation.
    ///          http://docs.amazonwebservices.com/AmazonS3/latest/API/index.html?RESTBucketPUTpolicy.html
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to set the policy for.</param>
    /// <param name="Policy">The policy, formatted as a JSON Object.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetBucketPolicy(const BucketName: string; Policy: TJSONObject;
                             ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Sets the logging state as well as any Grant information.</summary>
    /// <remarks>If LoggingInfo is nil, logging will be suspended for the given bucket.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to set the logging state for.</param>
    /// <param name="LoggingInfo">The logging info to set</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetBucketLogging(const BucketName: string; LoggingInfo: TAmazonBucketLoggingInfo;
                              ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Sets the notification events for the given bucket.</summary>
    /// <remarks>If Events is nil or an empty list, then notifications will be disabled for the bucket.
    ///          Note that currently only one event type is supported: s3:ReducedRedundancyLostObject
    ///          See the documentation on TAmazonNotificationEvent for more information.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to set the notification events for.</param>
    /// <param name="Events">The notification events to set</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <param name="BucketRegion">The optional region for the notification config</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetBucketNotification(const BucketName: string; Events: TList<TAmazonNotificationEvent>;
                                   ResponseInfo: TCloudResponseInfo = nil; BucketRegion: TAmazonChinaRegion = amzrNotSpecified): Boolean;
    /// <summary>Sets who pays for bucket requests.</summary>
    /// <remarks>If anything other than BucketOwner or Requester is passed as the Payer, the request will fail.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to set the Payer for.</param>
    /// <param name="Payer">The user who pays for bucket activity (BucketOwner, Requester)</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetBucketRequestPayment(const BucketName: string; Payer: TAmazonPayer;
                                     ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Enables or disables bucket versioning and MFA Delete.</summary>
    /// <remarks>To enable MFA (Multi-Factor-Authentication) Delete, the MFA published properties on the
    ///          Amazon Connection must be set. To enable the ability to use MFA and obtain the
    ///          serial key and token, log into your Amazon account and find the appropriate service.
    ///
    ///          Requests that with MFA (x-amz-mfa) must use HTTPS.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to set the versioning for.</param>
    /// <param name="Enabled">True to enable versioning, false to disable it.</param>
    /// <param name="MFADelete">True to Enable MFA delete, false to disable it.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetBucketVersioning(const BucketName: string; Enabled: Boolean; MFADelete: Boolean = False;
                                 ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Creates a new lifecycle configuration for the bucket or replaces an existing lifecycle configuration.</summary>
    /// <remarks>For this operation, a user must get the s3:PutLifecycleConfiguration permission.</remarks>
    /// <param name="ABucketName">The name of the bucket to set the lifecycle.</param>
    /// <param name="ALifeCycle">The lifecycle configuration to set.</param>
    /// <param name="AResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetBucketLifecycle(const ABucketName: string; const ALifeCycle: TAmazonLifeCycleConfiguration;
      const AResponseInfo: TCloudResponseInfo): boolean;
    /// <summary>Deletes the specified object from the given bucket.</summary>
    /// <remarks>Use this call when versioning is disabled on the bucket.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to delete the object from.</param>
    /// <param name="ObjectName">The name of the object to delete</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function DeleteObject(const BucketName, ObjectName: string;
                          ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Deletes the specified object's version from the given bucket.</summary>
    /// <remarks>You must be the bucket owner to make this call. If the specified version is a delete marker
    ///          and you have provided a ResponseInfo instance, then a 'x-amz-marker' header will be added
    ///          with a value of 'true'.
    ///
    ///          If MFA Delete is enabled then this call will need to be made over HTTPS and values
    ///          must be set on the connection for the MFA Serial Number and MFA Authentication Code.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket to delete the object version from.</param>
    /// <param name="ObjectName">The name of the object to delete a version of</param>
    /// <param name="VersionId">Id of the version to delete from the specified object.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function DeleteObjectVersion(const BucketName, ObjectName, VersionId: string;
                                 ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Deletes the lifecycle configuration from the specified bucket.</summary>
    /// <remarks>
    ///   To use this operation, you must have permission to perform the s3:PutLifecycleConfiguration action.
    ///   By default, the bucket owner has this permission and the bucket owner can grant this permission to others.
    /// </remarks>
    /// <param name="ABucketName">The name of the bucket to delete the lifecycle.</param>
    /// <param name="AResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function DeleteBucketLifecycle(const ABucketName: string; const AResponseInfo: TCloudResponseInfo): Boolean;
    /// <summary>Writes the stream for the given object, or nil.</summary>
    /// <remarks>You can use the OptionalParams instance to control the request.
    ///       The returned stream is nil if the request failed. If the request fails on a 404 (File Not Found)
    ///       error, then the requested object may have been deleted. If you provide a ResponseInfo instance,
    ///       then you can check if the object was deleted by verifying there is a 'x-amz-delete-marker'
    ///       response header, and its value is set to 'true'.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get</param>
    /// <param name="OptionalParams">The optional parameters/headers to use in the request.</param>
    /// <param name="ObjectStream">The stream to write to. Must not be nil.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The Object's stream or nil.</returns>
    function GetObject(const BucketName, ObjectName: string; OptionalParams: TAmazonGetObjectOptionals;
                       ObjectStream: TStream; ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Writes the stream for the given object, or nil.</summary>
    /// <remarks>If the request fails on a 404 (File Not Found) error, then the requested object may have
    ///          been deleted. If you provide a ResponseInfo instance, then you can check if the object was
    ///          deleted by verifying there is a 'x-amz-delete-marker' response header, and its value
    ///          is set to 'true'.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get</param>
    /// <param name="ObjectStream">The stream to write to. Must not be nil.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The Object's stream or nil.</returns>
    function GetObject(const BucketName, ObjectName: string;
                       ObjectStream: TStream; ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Writes the stream for the given object version, or nil.</summary>
    /// <remarks>See GetObject for more information.</remarks>
    /// <param name="BucketName"></param>
    /// <param name="ObjectName"></param>
    /// <param name="VersionId"></param>
    /// <param name="OptionalParams">The optional parameters/headers to use in the request.</param>
    /// <param name="ObjectStream">The stream to write to. Must not be nil.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The Object's stream or nil.</returns>
    function GetObjectVersion(const BucketName, ObjectName, VersionId: string;
                              OptionalParams: TAmazonGetObjectOptionals;
                              ObjectStream: TStream; ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Writes the stream for the given object version, or nil.</summary>
    /// <remarks>See GetObject for more information.</remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get a version of</param>
    /// <param name="VersionId">The Id of the version to get.</param>
    /// <param name="ObjectStream">The stream to write to. Must not be nil.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The Object's stream or nil.</returns>
    function GetObjectVersion(const BucketName, ObjectName, VersionId: string;
                              ObjectStream: TStream; ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>This returns the XML representation of the specified Object's ACL</summary>
    /// <remarks>To use this operation, you must have READ_ACP access to the object.</remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get the ACL for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the Object's Access Control Policy</returns>
    function GetObjectACLXML(const BucketName, ObjectName: string;
                             ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns the given object's ACL</summary>
    /// <remarks>For more information, see remarks on GetObjectACLXML.</remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get the ACL for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The object's ACL grant list</returns>
    function GetObjectACL(const BucketName, ObjectName: string;
                          ResponseInfo: TCloudResponseInfo = nil): TList<TAmazonGrant>;
    /// <summary>Writes the stream for the given object's torrent, or nil.</summary>
    /// <remarks>You can get torrent only for objects that are less than 5 GB in size.</remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get</param>
    /// <param name="ObjectStream">The stream to write to. Must not be nil.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The Object's stream or nil.</returns>
    function GetObjectTorrent(const BucketName, ObjectName: string; ObjectStream: TStream;
                              ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>This returns the metadata for the specified object.</summary>
    /// <remarks>An empty list will be returned if no metadata was included on the object.
    ///          The Response* fields of the OptionalParams instance are not used in this call.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get metadata for.</param>
    /// <param name="OptionalParams">The optional parameters/headers to use in the request.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The metadata key/value pairs, or nil if the request fails.</returns>
    function GetObjectMetadata(const BucketName, ObjectName: string; OptionalParams: TAmazonGetObjectOptionals;
                               ResponseInfo: TCloudResponseInfo = nil): TStrings; overload;
    /// <summary>This returns the metadata for the specified object.</summary>
    /// <remarks>An empty list will be returned if no metadata was included on the object.</remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get metadata for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The metadata key/value pairs, or nil if the request fails.</returns>
    function GetObjectMetadata(const BucketName, ObjectName: string;
                               ResponseInfo: TCloudResponseInfo = nil): TStrings; overload;

    /// <summary>This returns the properties and metadata for the specified object.</summary>
    /// <remarks>The Response* fields of the OptionalParams instance are not used in this call.
    ///          Empty lists will be returned if no metadata was included on the object.
    ///          The Response* fields of the OptionalParams instance are not used in this call.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get metadata for.</param>
    /// <param name="OptionalParams">The optional parameters/headers to use in the request.</param>
    /// <param name="Properties">The object's properties</param>
    /// <param name="Metadata">The object's metadata</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the request was successful, false otherwise.</returns>
    function GetObjectProperties(const BucketName, ObjectName: string;
                                 OptionalParams: TAmazonGetObjectOptionals;
                                 out Properties, Metadata: TStrings;
                                 ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>This returns the properties and metadata for the specified object.</summary>
    /// <remarks>Empty lists will be returned if no metadata was included on the object.
    ///          The Response* fields of the OptionalParams instance are not used in this call.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to get metadata for.</param>
    /// <param name="Properties">The object's properties</param>
    /// <param name="Metadata">The object's metadata</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the request was successful, false otherwise.</returns>
    function GetObjectProperties(const BucketName, ObjectName: string;
                                 out Properties, Metadata: TStrings;
                                 ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Uploads the given object, optionally setting metadata on it.</summary>
    /// <remarks>Several optional headers can be set to the request. To see a full list, view the Amazon API:
    ///          http://docs.amazonwebservices.com/AmazonS3/latest/API/index.html?RESTObjectPUT.html
    ///          Some include: Content-MD5, Content-Type, x-amz-storage-class and several more.
    ///
    ///          If you provide a ResponseInfo instance and versioning is enabled, then a successful request
    ///          will result in a 'x-amz-version-id' header being populated, which is the uploaded object's
    ///          version.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name to use for the object being uploaded.</param>
    /// <param name="Content">The Object's content, in bytes.</param>
    /// <param name="ReducedRedundancy">True to use REDUCED_REDUNDANCY as the object's storage class.</param>
    /// <param name="Metadata">The optional metadata to set on the object, or nil.</param>
    /// <param name="Headers">Optional request headers to use. See remarks.</param>
    /// <param name="ACL">Optional ACL to apply to the object. If unspecified, will default to private.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the request was successful, false otherwise.</returns>
    function UploadObject(const BucketName, ObjectName: string; Content: TArray<Byte>; ReducedRedundancy: Boolean = false;
                          Metadata: TStrings = nil;
                          Headers: TStrings = nil; ACL: TAmazonACLType = amzbaPrivate;
                          ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Sets the ACL for the given object.</summary>
    /// <remarks>The given ACP holds the owner information as well as the ACL.
    ///
    ///          Several optional headers can be set to the request. To see a full list, view the Amazon API:
    ///          http://docs.amazonwebservices.com/AmazonS3/latest/API/index.html?RESTObjectPUTacl.html
    ///          Some include: Content-MD5, Content-Type, x-amz-storage-class and several more.
    ///
    ///          If you provide a ResponseInfo instance and versioning is enabled, then a successful request
    ///          will result in a 'x-amz-version-id' header being populated, which is the updated object's
    ///          version.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to set the ACL for.</param>
    /// <param name="ACP">The access control policy containing owner info and the ACL.</param>
    /// <param name="Headers">Optional request headers to use. See remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetObjectACL(const BucketName, ObjectName: string; ACP: TAmazonAccessControlPolicy;
                          Headers: TStrings = nil; ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Sets the ACL for the given object.</summary>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The name of the object to set the ACL for.</param>
    /// <param name="ACL">The ACL to apply to the object. If unspecified, will default to private.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetObjectACL(const BucketName, ObjectName: string; ACL: TAmazonACLType;
                          ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Copies the specified source object to the given target object.</summary>
    /// <remarks>The OptionalParams instance can be set to provide more control over the request.
    ///          If you provide a ResponseInfo instance, then you can check the value of the
    ///          'x-amz-version-id' header to get the VersionId of the resulting (target) object if
    ///          versioning is enabled. If versioning is enabled then this copy will copy the most
    ///          recent soruce object. The 'x-amz-copy-source-version-id' result header will specify
    ///          the VersionId of the source object that was copied.
    ///          See the comments on TAmazonCopyObjectOptionals for more information.
    ///
    ///          You can change the storage class of an existing object by copying it to the same name
    ///          in the same bucket. To do that, you use the following request optional parameter:
    ///             x-amz-storage-class set to STANDARD or REDUCED_REDUNDANCY
    /// </remarks>
    /// <param name="DestinationBucket">The bucket the object will be copied into.</param>
    /// <param name="DestinationObjectName">The name of the resulting object after the copy.</param>
    /// <param name="SourceBucket">The bucket the object being copied is in.</param>
    /// <param name="SourceObjectName">The name of the object being copied.</param>
    /// <param name="OptionalParams">Optional parameters to refine the request. See remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function CopyObject(const DestinationBucket, DestinationObjectName: string;
                        const SourceBucket, SourceObjectName: string;
                        OptionalParams: TAmazonCopyObjectOptionals = nil;
                        ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Sets the metadata on the given object.</summary>
    /// <remarks>This performs a copy object, with the source and destination the same.
    ///          Any previous metadata on the object will be lost.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The object to set the metadata for</param>
    /// <param name="Metadata">The metadata to set on the object</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function SetObjectMetadata(const BucketName, ObjectName: string; Metadata: TStrings;
                               ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Copies the specified source object's version to the given target object.</summary>
    /// <remarks>See the comments on CopyObject for more information.
    /// </remarks>
    /// <param name="DestinationBucket">The bucket the object will be copied into.</param>
    /// <param name="DestinationObjectName">The name of the resulting object after the copy.</param>
    /// <param name="SourceBucket">The bucket the object being copied is in.</param>
    /// <param name="SourceObjectName">The name of the object being copied.</param>
    /// <param name="SourceVersionId">The Version of the object to copy</param>
    /// <param name="OptionalParams">Optional parameters to refine the request. See remarks.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function CopyObjectVersion(const DestinationBucket, DestinationObjectName: string;
                               const SourceBucket, SourceObjectName, SourceVersionId: string;
                               OptionalParams: TAmazonCopyObjectOptionals = nil;
                               ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Start a new multipart upload.</summary>
    /// <remarks>The XML returned contains the UploadId. This is required for future calls to 'UploadPart'
    ///          or for stopping/cancelling the multipart upload.
    ///
    ///          There are several supported optional parameters. For a list of them and their functionality,
    ///          go to the Amazon documentation:
    ///
    ///          http://docs.amazonwebservices.com/AmazonS3/latest/API/index.html?mpUploadInitiate.html
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object will be in.</param>
    /// <param name="ObjectName">The name of the object this multipart upload will create.</param>
    /// <param name="Metadata">The metadata to set on the resulting object, or nil.</param>
    /// <param name="Headers">Optional headers to set. See remarks.</param>
    /// <param name="ACL">Optional ACL to set on the resulting object.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>XML containing the UploadId to use for subsequent calls.</returns>
    function InitiateMultipartUploadXML(const BucketName, ObjectName: string; Metadata: TStrings = nil;
                                        Headers: TStrings = nil;
                                        ACL: TAmazonACLType = amzbaPrivate;
                                        ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Start a new multipart upload.</summary>
    /// <remarks>See comments on InitiateMultipartUploadXML for more information.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the object will be in.</param>
    /// <param name="ObjectName">The name of the object this multipart upload will create.</param>
    /// <param name="Metadata">The metadata to set on the resulting object, or nil.</param>
    /// <param name="Headers">Optional headers to set. See remarks.</param>
    /// <param name="ACL">Optional ACL to set on the resulting object.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The UploadId to use for subsequent calls.</returns>
    function InitiateMultipartUpload(const BucketName, ObjectName: string; Metadata: TStrings = nil;
                                     Headers: TStrings = nil;
                                     ACL: TAmazonACLType = amzbaPrivate;
                                     ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Aborts a previously initiated multipart upload.</summary>
    /// <remarks>All storage consumed by previously uplaoded parts for this multipart upload will be freed.
    ///          However, if there are any in-progress part uploads for this UploadId when you abort it,
    ///          then the part may be uploaded successfully and you would then be required to
    ///          abort the UploadId again to free any additional parts.
    /// </remarks>
    /// <param name="BucketName">The bucket the multipart upload object was to be stored.</param>
    /// <param name="ObjectName">The name of the object that would have resulted from the multipart upload.</param>
    /// <param name="UploadId">The UploadId originally returned when the multipart upload was initiated.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function AbortMultipartUpload(const BucketName, ObjectName, UploadId: string;
                                  ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Uploads a part to an initiated multipart upload.</summary>
    /// <remarks>All parts except the last part must be at least 5 MB in size.
    ///          Part numbers can be any number from 1 to 10,000, inclusive. If you specify a part number that
    ///          already had been uploaded, the content will be replaced by this content.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the multipart upload's object is for.</param>
    /// <param name="ObjectName">The name of the multipart upload's object.</param>
    /// <param name="UploadId">The multipart upload's unique Id.</param>
    /// <param name="PartNumber">The part number to assign to this content.</param>
    /// <param name="Content">The content to upload.</param>
    /// <param name="Part">The part result (ETag and Number) if the request was successful.</param>
    /// <param name="ContentMD5">The optional MD5 of the content being sent, for integrity checking.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if successful, false otherwise.</returns>
    function UploadPart(const BucketName, ObjectName, UploadId: string; PartNumber: Integer; Content: TArray<Byte>;
                        out Part: TAmazonMultipartPart;
                        const ContentMD5: string = '';
                        ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Completes the given multipart upload, committing the specified parts.</summary>
    /// <param name="BucketName">The name of the bucket the object is in.</param>
    /// <param name="ObjectName">The object the multipart upload is for.</param>
    /// <param name="UploadId">The multipart upload's unique Id.</param>
    /// <param name="Parts">The list of parts to build the resulting object from.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>true if successful, false otherwise.</returns>
    function CompleteMultipartUpload(const BucketName, ObjectName, UploadId: string;
                                     Parts: TList<TAmazonMultipartPart>;
                                     ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Lists the currently uploaded parts for multipart upload with the given ID.</summary>
    /// <remarks>MaxParts can be set to a number from 2 to 1000. Set it to 0 to use the server default value.
    ///          PartNumberMarker is the continuation token returned from a previous call, in the XML element
    ///          with the same name.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the multipart upload is for.</param>
    /// <param name="ObjectName">The name of the object the multipart upload is for.</param>
    /// <param name="UploadId">The UploadId identifying the multipart upload.</param>
    /// <param name="MaxParts">The maximum number of parts to return, or 0 for server default.</param>
    /// <param name="PartNumberMarker">The part number to continue population from,
    ///                                or 0 to start from the beginning.
    /// </param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the multipart upload parts.</returns>
    function ListMultipartUploadPartsXML(const BucketName, ObjectName, UploadId: string;
                                         MaxParts: Integer = 0; PartNumberMarker: Integer = 0;
                                         ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Lists the currently uploaded parts for multipart upload with the given ID.</summary>
    /// <remarks>MaxParts can be set to a number from 2 to 1000. Set it to 0 to use the server default value.
    ///          PartNumberMarker is the continuation token returned from a previous call, in the property
    ///          with the same name.
    /// </remarks>
    /// <param name="BucketName">The name of the bucket the multipart upload is for.</param>
    /// <param name="ObjectName">The name of the object the multipart upload is for.</param>
    /// <param name="UploadId">The UploadId identifying the multipart upload.</param>
    /// <param name="MaxParts">The maximum number of parts to return, or 0 for server default.</param>
    /// <param name="PartNumberMarker">The part number to continue population from,
    ///                                or 0 to start from the beginning.
    /// </param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The list of multipart upload parts and additional metadata, or nil if the request fails.
    /// </returns>
    function ListMultipartUploadParts(const BucketName, ObjectName, UploadId: string;
                                      MaxParts: Integer = 0; PartNumberMarker: Integer = 0;
                                      ResponseInfo: TCloudResponseInfo = nil): TAmazonListPartsResult;
  end;


implementation

uses
  System.Hash, System.NetConsts, System.Net.HTTPClient,
  System.TypInfo, System.DateUtils, Xml.XMLDoc, System.StrUtils,
  System.NetEncoding, System.Net.URLClient
{$IFDEF MACOS}
    , Macapi.CoreFoundation
{$ENDIF MACOS}
    ;

{ TAmazonChinaStorageService }

constructor TAmazonChinaStorageService.Create(const ConnectionInfo: TAmazonChinaConnectionInfo);
begin
  inherited Create(ConnectionInfo);

  FUseCanonicalizedHeaders := True;
  FUseResourcePath := True;

  //The QueryPrefix is on the same line for S3, so re-add the question mark
  FQueryStartChar := '?';
end;

procedure TAmazonChinaStorageService.URLEncodeQueryParams(const ForURL: Boolean; var ParamName, ParamValue: string);
begin
  if ForURL then
    inherited;
end;

function TAmazonChinaStorageService.CreateAuthInstance(const ConnectionInfo: TAmazonConnectionInfo): TCloudAuthentication;
begin
  Result := TAmazonAWS4Authentication.Create(ConnectionInfo,True); //S3 uses HMAC-SHA256
end;

function TAmazonChinaStorageService.BuildStringToSignHeaders(Headers: TStringList): string;
var
  RequiredHeadersInstanceOwner: Boolean;
  RequiredHeaders: TStringList;
  I, ReqCount: Integer;
  Aux: string;
  lastParam, nextParam, ConHeadPrefix: string;
begin
  //AWS always has required headers
  RequiredHeaders := TStringList(GetRequiredHeaderNames(RequiredHeadersInstanceOwner));
  Assert(RequiredHeaders <> nil);
  Assert(Headers <> nil);
  //if (Headers = nil) then
   // Headers.AddStrings(RequiredHeaders);

  //AWS4 - content-type must be included in string to sign if found in headers
  if Headers.IndexOfName('content-type') > -1 then //Headers.Find('content-type',Index) then
    RequiredHeaders.Add('content-type');
  if Headers.IndexOfName('content-md5') > -1 then
    RequiredHeaders.Add('content-md5');
  RequiredHeaders.Sorted := True;
  RequiredHeaders.Duplicates := TDuplicates.dupIgnore;
  ConHeadPrefix := AnsiLowerCase(GetCanonicalizedHeaderPrefix);
  for I := 0 to Headers.Count - 1 do
  begin
    Aux := AnsiLowerCase(Headers.Names[I]);
    if AnsiStartsStr(ConHeadPrefix, Aux) then
      RequiredHeaders.Add(Aux);
  end;
  RequiredHeaders.Sorted := False;
  //custom sorting
  SortHeaders(RequiredHeaders);
  ReqCount := RequiredHeaders.Count;

   //AWS4 get value pairs (ordered + lowercase)
  if FUseCanonicalizedHeaders and (Headers <> nil) then
  begin
    for I := 0 to ReqCount - 1 do
    begin
      Aux := AnsiLowerCase(RequiredHeaders[I]);
      if Headers.IndexOfName(Aux) < 0 then
        raise Exception.Create('Missing Required Header: '+RequiredHeaders[I]);
      nextParam := Aux;
      if lastParam = EmptyStr then
      begin
        lastParam := nextParam;
        Result := Result + Format('%s:%s', [nextParam, Headers.Values[lastParam]]);
      end
      else
      begin
        lastParam := nextParam;
        Result := Result + Format(#10'%s:%s', [nextParam, Headers.Values[lastParam]]);
      end;
    end;
    if lastParam <> EmptyStr then
      Result := Result + #10'';
  end;

  // string of header names
  Result := Result + #10'';
  for I := 0 to ReqCount - 1 do
  begin
    Result := Result + Format('%s;', [RequiredHeaders[I]]);
  end;
  SetLength(Result,Length(Result)-1);

  if RequiredHeadersInstanceOwner then
    FreeAndNil(RequiredHeaders);
end;

function TAmazonChinaStorageService.GetCanonicalizedHeaderPrefix: string;
begin
  Result := 'x-amz-';
end;

function TAmazonChinaStorageService.GetNotificationXML(Events: TList<TAmazonNotificationEvent>): string;
var
  sb: TStringBuilder;
  Event: TAmazonNotificationEvent;
begin
  if (Events = nil) or (Events.Count = 0) then
    Exit('<NotificationConfiguration />');

  sb := TStringBuilder.Create;

  try
    sb.Append('<NotificationConfiguration>');

    for Event In Events do
    begin
      sb.Append('<TopicConfiguration><Topic>');
      sb.Append(Event.Topic);
      sb.Append('</Topic><Event>');
      sb.Append(Event.Event);
      sb.Append('</Event></TopicConfiguration>');
    end;

    sb.Append('</NotificationConfiguration>');
  finally
    Result := sb.ToString;
    FreeAndNil(sb);
  end;
end;

function TAmazonChinaStorageService.GetVirtualHostFromRegion(const bucketname: string; location: TAmazonChinaRegion): string;
begin
  Result := Format('%s.%s', [bucketname, GetEndpointFromRegion(TAmazonChinaServiceType.csS3 ,location)]);
end;

function TAmazonChinaStorageService.GetRequiredHeaderNames(out InstanceOwner: Boolean): TStrings;
begin
  InstanceOwner := False;
  if (FRequiredHeaderNames = nil) or (FRequiredHeaderNames.Count = 0) then
  begin
    FRequiredHeaderNames.Free;
    FRequiredHeaderNames := TStringList.Create;
    FRequiredHeaderNames.Add('host');
    FRequiredHeaderNames.Add('x-amz-content-sha256');
    FRequiredHeaderNames.Add('x-amz-date');
  end;
  Result := FRequiredHeaderNames;
end;

procedure TAmazonChinaStorageService.AddAndValidateHeaders(const defaultHeaders, customHeaders: TStrings);
var
  IsInstanceOwner: Boolean;
  RequiredHeaders: TStrings;
  I: Integer;
begin
  RequiredHeaders :=  GetRequiredHeaderNames(IsInstanceOwner);
  for I := 0 to customHeaders.Count - 1 do
  begin
    if not (RequiredHeaders.IndexOfName(customHeaders.Names[I]) > -1) then
       defaultHeaders.Append(customHeaders[I]);
  end;
  if IsInstanceOwner then
    FreeAndNil(RequiredHeaders);
end;

function TAmazonChinaStorageService.InitHeaders(const BucketName: string): TStringList;
begin
  Result := TStringList.Create;
  Result.CaseSensitive := false;
  Result.Duplicates := TDuplicates.dupIgnore;
  Result.Values['host'] := GetConnectionInfo.VirtualHost(BucketName);
  Result.Values['x-amz-content-sha256'] := 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'; //empty string
  Result.Values['x-amz-date'] := ISODateTime_noSeparators;
end;

procedure TAmazonChinaStorageService.PopulateGrants(GrantsNode: IXMLNode; Grants: TList<TAmazonGrant>);
var
  GrantNode, GranteeNode, Aux: IXMLNode;
  PermissionStr, GranteeID, GranteeDisplayName: string;
  Grant: TAmazonGrant;
begin
  GrantNode := GrantsNode.ChildNodes.First;

  while GrantNode <> nil do
  begin
    GranteeDisplayName := EmptyStr;
    GranteeNode := GrantNode.ChildNodes.FindNode('Grantee');

    Aux := GrantNode.ChildNodes.FindNode('Permission');
    if (Aux <> nil) and Aux.IsTextElement then
    begin
      PermissionStr := Aux.Text;

      if (GranteeNode <> nil) and GranteeNode.HasChildNodes then
      begin
        Aux := GranteeNode.ChildNodes.FindNode('URI');
        if (Aux <> nil) and Aux.IsTextElement then
        begin
          Grant := TAmazonGrant.Create(PermissionStr);
          Grant.GranteeURI := Aux.Text;
          Grants.Add(Grant);
        end
        else
        begin
          Aux := GranteeNode.ChildNodes.FindNode('EmailAddress');
          if (Aux <> nil) and Aux.IsTextElement then
          begin
            Grant := TAmazonGrant.Create(PermissionStr);
            Grant.GranteeEmailAddress := Aux.Text;
            Grants.Add(Grant);
          end
          else
          begin
            Aux := GranteeNode.ChildNodes.FindNode('ID');
            if (Aux <> nil) and Aux.IsTextElement then
            begin
              GranteeID := Aux.Text;
              Aux := GranteeNode.ChildNodes.FindNode('DisplayName');
              if (Aux <> nil) and Aux.IsTextElement then
                GranteeDisplayName := Aux.Text;

              Grant := TAmazonGrant.Create(PermissionStr);
              Grant.GranteeID := GranteeID;
              Grant.GranteeDisplayName := GranteeDisplayName;
              Grants.Add(Grant);
            end;
          end;
        end;
      end;
    end;
    GrantNode := GrantNode.NextSibling;
  end;
end;

function TAmazonChinaStorageService.PopulateResultItem(ObjectNode: IXMLNode;
                                                  out ResultItem: TAmazonObjectResult): Boolean;
var
  ItemNode, Aux: IXMLNode;
  NodeName: string;
begin
  Result := False;
  if (ObjectNode <> nil) and ObjectNode.HasChildNodes then
  begin
    ItemNode := ObjectNode.ChildNodes.First;

    while ItemNode <> nil do
    begin
      NodeName := ItemNode.NodeName;
      if AnsiSameText(NodeName, 'Key') then
        ResultItem.Name := ItemNode.Text
      else if AnsiSameText(NodeName, 'LastModified') then
        ResultItem.LastModified := ItemNode.Text
      else if AnsiSameText(NodeName, 'ETag') then
        ResultItem.ETag := ItemNode.Text
      else if AnsiSameText(NodeName, 'StorageClass') then
        ResultItem.StorageClass := ItemNode.Text
      else if AnsiSameText(NodeName, 'VersionId') then
        ResultItem.VersionId := ItemNode.Text
      else if AnsiSameText(NodeName, 'IsLatest') then
        ResultItem.IsLatest := AnsiSameText(ItemNode.Text, 'true')
      else if AnsiSameText(NodeName, 'Size') then
        ResultItem.Size := StrToInt64(ItemNode.Text)
      else if AnsiSameText(NodeName, 'Owner') then
      begin
        Aux := ItemNode.ChildNodes.FindNode('ID');
        if (Aux <> nil) and Aux.IsTextElement then
          ResultItem.OwnerID := Aux.Text;

        Aux := ItemNode.ChildNodes.FindNode('DisplayName');
        if (Aux <> nil) and Aux.IsTextElement then
          ResultItem.OwnerDisplayName := Aux.Text;
      end;

      ItemNode := ItemNode.NextSibling;
    end;

    Result := ResultItem.Name <> EmptyStr;
  end;
end;

procedure TAmazonChinaStorageService.ParseResponseError(const ResponseInfo: TCloudResponseInfo;
                                                   const ResultString: string);
var
  xmlDoc: IXMLDocument;
  Aux, ErrorNode, MessageNode: IXMLNode;
  ErrorCode, ErrorMsg: string;
begin
  //If the ResponseInfo instance exists (to be populated) and the passed in string is Error XML, then
  //continue, otherwise exit doing nothing.
  if (ResponseInfo = nil) or (ResultString = EmptyStr) then
    Exit;

  if (AnsiPos('<Error', ResultString) > 0) then
  begin
    xmlDoc := TXMLDocument.Create(nil);
    try
      xmlDoc.LoadFromXML(ResultString);
    except
      //Response content isn't XML
      Exit;
    end;

    //Amazon has different formats for returning errors as XML
    ErrorNode := xmlDoc.DocumentElement;

    if (ErrorNode <> nil) and (ErrorNode.HasChildNodes) then
    begin
      MessageNode := ErrorNode.ChildNodes.FindNode(NODE_ERROR_MESSAGE);

      if (MessageNode <> nil) then
        ErrorMsg := MessageNode.Text;

      if ErrorMsg <> EmptyStr then
      begin
        //Populate the error code
        Aux := ErrorNode.ChildNodes.FindNode(NODE_ERROR_CODE);
        if (Aux <> nil) then
          ErrorCode := Aux.Text;
        ResponseInfo.StatusMessage := Format('%s - %s (%s)', [ResponseInfo.StatusMessage, ErrorMsg, ErrorCode]);
      end;
    end;
  end
end;

function TAmazonChinaStorageService.CurrentTime: string;
begin
  Result := FormatDateTime('ddd, dd mmm yyyy hh:nn:ss "GMT"',
                           TTimeZone.Local.ToUniversalTime(Now),
                           TFormatSettings.Create('en-US'));
end;

destructor TAmazonChinaStorageService.Destroy;
begin
  FreeAndNil(FRequiredHeaderNames);
  inherited;
end;

function TAmazonChinaStorageService.CreateBucket(const BucketName: string; BucketACL: TAmazonACLType;
                                            BucketRegion: TAmazonChinaRegion;
                                            ResponseInfo: TCloudResponseInfo): Boolean;
var
  BucketACLStr: string;
  RegionStr: string;
  url, virtualhost: string;
  contentStream: TStringStream;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  RespStr: string;
  ContentStr: string;
  ContentLength: Integer;
begin
  contentStream := nil;

  Headers := InitHeaders(BucketName);
  BucketACLStr := GetACLTypeString(BucketACL);

  if GetConnectionInfo.UseDefaultEndpoints then
  begin
    RegionStr := TAmazonChinaRegions[BucketRegion];
    url := GetConnectionInfo.StorageURL(BucketName) + '/';
  end
  else
  begin
    RegionStr := GetRegionFromEndpoint(TAmazonChinaServiceType.csS3 ,GetConnectionInfo.StorageEndpoint);  //create bucket in same region as endpoint
    virtualhost:= BucketName + '.' + GetConnectionInfo.StorageEndpoint;
    url := Format('%s://%s/', [GetConnectionInfo.Protocol, virtualhost]);
    Headers.Values['host'] := virtualhost;
  end;

  //Optionally add in the ACL value
  if not BucketACLStr.IsEmpty then
    Headers.Values['x-amz-acl'] := BucketACLStr;

  if not RegionStr.Equals('us-east-1') then
  begin
    ContentStr := '<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">' +
                    '<LocationConstraint>' + RegionStr + '</LocationConstraint>' +
                  '</CreateBucketConfiguration>';

    contentStream := TStringStream.Create;
    contentStream.WriteString(ContentStr);

    ContentLength := contentStream.Size;
    contentStream.position := 0;
  end
  else
    ContentLength := 0;

  if ContentLength > 0 then
     Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(ContentStr);

  Headers.Values['Content-Length'] := IntToStr(ContentLength);

  Headers.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';

  QueryPrefix := '/' + BucketName + '/';

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, RespStr);
    ParseResponseError(ResponseInfo, RespStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(contentStream);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.ListBucketsXML(const ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  url := GetConnectionInfo.StorageURL;

  Headers := InitHeaders;

  QueryPrefix := '/';

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.ListBuckets(const ResponseInfo: TCloudResponseInfo): TStrings;
var
  xml: string;
  xmlDoc: IXMLDocument;
  ResultNode, BucketNode, Aux: IXMLNode;
  Name: string;
begin
  Result := nil;
  xml := ListBucketsXML(ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit(Result);
    end;

    ResultNode := xmlDoc.DocumentElement.ChildNodes.FindNode('Buckets');

    if ResultNode <> nil then
    begin
      Result := TStringList.Create;

      if not ResultNode.HasChildNodes then
        Exit(Result);

      BucketNode := ResultNode.ChildNodes.First;

      while BucketNode <> nil do
      begin
        if AnsiSameText(BucketNode.NodeName, 'Bucket') and BucketNode.HasChildNodes then
        begin
          Aux := BucketNode.ChildNodes.FindNode('Name');
          if (Aux <> nil) and Aux.IsTextElement then
          begin
            Name := Aux.Text;
            Aux := BucketNode.ChildNodes.FindNode('CreationDate');
            if (Aux <> nil) and Aux.IsTextElement then
            begin
              Result.Values[Name] := Aux.Text;
            end;
          end;
        end;
        BucketNode := BucketNode.NextSibling;
      end;
    end;
  end;
end;


function TAmazonChinaStorageService.DeleteBucket(const BucketName: string; ResponseInfo: TCloudResponseInfo;
                                           BucketRegion: TAmazonChinaRegion): Boolean;
var
  url, virtualhost: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  RespStr: string;
begin
  Response := nil;
  Headers := nil;
  try
    Headers := InitHeaders(BucketName);

    if not GetConnectionInfo.UseDefaultEndpoints then
    begin
      virtualhost := BucketName + '.' + GetConnectionInfo.StorageEndpoint;
      url := Format('%s://%s/', [GetConnectionInfo.Protocol, virtualhost]);
      Headers.Values['host'] := virtualhost;
    end else
    if not GetRegionFromEndpoint(TAmazonChinaServiceType.csS3, GetConnectionInfo.StorageEndpoint).Equals(TAmazonChinaRegions[BucketRegion]) then
    begin
      virtualhost := GetVirtualHostFromRegion(BucketName, BucketRegion);
      url := Format('%s://%s/', [GetConnectionInfo.Protocol, virtualhost]);
      Headers.Values['host'] := virtualhost;
    end
    else
      url := GetConnectionInfo.StorageURL(BucketName) + '/';

    QueryPrefix := '/' + BucketName + '/';

    Response := IssueDeleteRequest(url, Headers, nil, QueryPrefix, ResponseInfo, RespStr);
    ParseResponseError(ResponseInfo, RespStr);
    Result := (Response <> nil) and (Response.ResponseCode = 204);
  finally
    Response.Free;
    Headers.Free;
  end;
end;

function TAmazonChinaStorageService.DeleteBucketPolicy(const BucketName: string;
                                                  ResponseInfo: TCloudResponseInfo; BucketRegion: TAmazonChinaRegion): Boolean;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  RespStr: string;
begin
  url := GetConnectionInfo.StorageURL(BucketName) + '/?policy=';

  Headers := InitHeaders(BucketName);

  QueryPrefix := '/' + BucketName + '/?policy';

  Response := nil;
  try
    Response := IssueDeleteRequest(url, Headers, nil, QueryPrefix, ResponseInfo, RespStr);
    ParseResponseError(ResponseInfo, RespStr);
    Result := (Response <> nil) and (Response.ResponseCode = 204);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetBucketXML(const BucketName: string; OptionalParams: TStrings;
                                            ResponseInfo: TCloudResponseInfo; BucketRegion: TAmazonChinaRegion): string;
begin
  Result := GetBucketXMLInternal(BucketName, OptionalParams, False, ResponseInfo, BucketRegion);
end;

function TAmazonChinaStorageService.GetBucketXMLInternal(const BucketName: string; OptionalParams: TStrings;
                                          VersionInfo: Boolean; ResponseInfo: TCloudResponseInfo; BucketRegion: TAmazonChinaRegion): string;
var
  url, virtualhost: string;
  Headers: TStringList;
  QueryParams: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  QueryParams := nil;
  Response := nil;
  try
    Headers := InitHeaders(BucketName);

    if not GetConnectionInfo.UseDefaultEndpoints then
    begin
      virtualhost := BucketName + '.' + GetConnectionInfo.StorageEndpoint;
      url := Format('%s://%s/', [GetConnectionInfo.Protocol, virtualhost]);
      Headers.Values['host'] := virtualhost;
    end else
    if not GetRegionFromEndpoint(TAmazonChinaServiceType.csS3, GetConnectionInfo.StorageEndpoint).Equals(TAmazonChinaRegions[BucketRegion]) then
    begin
      virtualhost := GetVirtualHostFromRegion(BucketName, BucketRegion);
      url := Format('%s://%s/', [GetConnectionInfo.Protocol, virtualhost]);
      Headers.Values['host'] := virtualhost;
    end
    else
      url := GetConnectionInfo.StorageURL(BucketName) + '/';

    if VersionInfo then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.Values['versions'] := ' ';
    end;

    if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.AddStrings(OptionalParams);
    end;

    if QueryParams <> nil then
      url := BuildQueryParameterString(url, QueryParams, False, True);

    QueryPrefix := '/' + BucketName + '/';

    if VersionInfo then
      QueryPrefix := QueryPrefix + '?versions';

    Response := IssueGetRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    QueryParams.Free;
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetBucket(const BucketName: string; OptionalParams: TStrings;
                                         ResponseInfo: TCloudResponseInfo; BucketRegion: TAmazonChinaRegion): TAmazonBucketResult;
var
  xml: string;
begin
  xml := GetBucketXML(BucketName, OptionalParams, ResponseInfo, BucketRegion);
  Result := GetBucketInternal(xml, ResponseInfo);
end;

function TAmazonChinaStorageService.GetBucketInternal(const XML: string;
                                                 ResponseInfo: TCloudResponseInfo): TAmazonBucketResult;
var
  xmlDoc: IXMLDocument;
  RootNode, ItemNode, Aux: IXMLNode;
  NodeName: string;
  ResultItem: TAmazonObjectResult;
  IsVersionRequest: Boolean;
begin
  Result := nil;

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit(nil);
    end;

    RootNode := xmlDoc.DocumentElement;

    IsVersionRequest := AnsiSameText(RootNode.NodeName, 'ListVersionsResult');

    //if it isn't a bucket content request or a version request, return nil
    if not (IsVersionRequest or AnsiSameText(RootNode.NodeName, 'ListBucketResult')) then
      Exit(nil);

    ItemNode := RootNode.ChildNodes.FindNode('Name');

    if (ItemNode = nil) or (not ItemNode.IsTextElement) then
      Exit(nil);

    Result := TAmazonBucketResult.Create(ItemNode.Text);

    ItemNode := RootNode.ChildNodes.First;

    while ItemNode <> nil do
    begin
      NodeName := ItemNode.NodeName;
      if AnsiSameText(NodeName, 'Prefix') then
        Result.RequestPrefix := ItemNode.Text
      else if AnsiSameText(NodeName, 'Delimiter') then
        Result.RequestDelimiter := ItemNode.Text
      else if AnsiSameText(NodeName, 'Marker') or AnsiSameText(NodeName, 'KeyMarker') then
        Result.Marker := ItemNode.Text
      else if IsVersionRequest and AnsiSameText(NodeName, 'VersionIdMarker')  then
        Result.VersionIdMarker := ItemNode.Text
      else if AnsiSameText(NodeName, 'MaxKeys') then
        try
          Result.RequestMaxKeys := StrToInt(ItemNode.Text);
        except
        end
      else if AnsiSameText(NodeName, 'IsTruncated') then
        Result.IsTruncated := not AnsiSameText(ItemNode.Text, 'false')
      else if AnsiSameText(NodeName, 'Contents') or AnsiSameText(NodeName, 'Version') then
      begin
        if PopulateResultItem(ItemNode, ResultItem) then
          Result.Objects.Add(ResultItem);
      end
      else if AnsiSameText(NodeName, 'CommonPrefixes') then
      begin
        Aux := ItemNode.ChildNodes.First;

        while Aux <> nil do
        begin
          if AnsiSameText(Aux.NodeName, 'Prefix') and Aux.IsTextElement then
            Result.Prefixes.Add(Aux.Text);
          Aux := Aux.NextSibling;
        end;
      end;

      ItemNode := ItemNode.NextSibling;
    end;

    //populate the appropriate marker header values, depending on if it was a Content or Version population
    if Result.IsTruncated and (ResponseInfo <> nil) and (Result.Objects.Count > 1) then
    begin
      ResultItem := Result.Objects.Last;

      //If it was a version population, all objects will have a VersionId
      //and the marker parameters for a subsequent request will be different.
      if IsVersionRequest then
      begin
        ResponseInfo.Headers.Values['key-marker'] := ResultItem.Name;
        ResponseInfo.Headers.Values['version-id-marker'] := ResultItem.VersionId;
      end
      else
        ResponseInfo.Headers.Values['marker'] := ResultItem.Name;
    end;
  end;
end;

function TAmazonChinaStorageService.GetBucketACLXML(const BucketName: string;
                                               ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  Headers := InitHeaders(BucketName);

  url := GetConnectionInfo.StorageURL(BucketName) + '/?acl=';

  QueryPrefix := '/' + BucketName + '/?acl';

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetBucketACL(const BucketName: string;
                                            ResponseInfo: TCloudResponseInfo): TList<TAmazonGrant>;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ListNode: IXMLNode;
begin
  Result := nil;
  xml := GetBucketACLXML(BucketName, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit(nil);
    end;

    RootNode := xmlDoc.DocumentElement;

    if not AnsiSameText(RootNode.NodeName, 'AccessControlPolicy') then
      Exit(nil);

    Result := TList<TAmazonGrant>.Create;
    ListNode := RootNode.ChildNodes.FindNode('AccessControlList');

    if (ListNode = nil) then
      Exit(Result);

    PopulateGrants(ListNode, Result);
  end;
end;

function TAmazonChinaStorageService.GetBucketPolicyJSON(const BucketName: string;
                                                   ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  url := GetConnectionInfo.StorageURL(BucketName) + '/?policy=';

  Headers := InitHeaders(BucketName);

  QueryPrefix := '/' + BucketName + '/?policy';

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetBucketPolicy(const BucketName: string;
                                               ResponseInfo: TCloudResponseInfo): TJSONObject;
var
  jsonString: string;
begin
  Result := nil;
  jsonString := GetBucketPolicyJSON(BucketName, ResponseInfo);

  if AnsiStartsStr('{', jsonString) then
  begin
    try
      Result := TJSONObject(TJSONObject.ParseJSONValue(jsonString));
    except
      Result := nil;
    end;
  end;
end;

function TAmazonChinaStorageService.GetBucketLocationXML(const BucketName: string;
                                                    ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin

  Headers := InitHeaders(BucketName);
  url := Format('%s://%s', [GetConnectionInfo.Protocol, GetConnectionInfo.StorageEndpoint]) + '/'+ BucketName + '/?location=';
  Headers.Values['host'] := GetConnectionInfo.StorageEndpoint;

  //url := GetConnectionInfo.StorageURL(BucketName) + '/?location';

  QueryPrefix := '/' + BucketName + '/?location';

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetBucketLocation(const BucketName: string;
                                                 ResponseInfo: TCloudResponseInfo): TAmazonChinaRegion;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode: IXMLNode;
begin
  Result := amzrNotSpecified;
  xml := GetBucketLocationXML(BucketName, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit;
    end;

    RootNode := xmlDoc.DocumentElement;

    if AnsiSameText(RootNode.NodeName, 'LocationConstraint')  then
    begin
      if RootNode.IsTextElement then
        Exit(GetRegionFromString(RootNode.Text));

      Exit(TAmazonChinaRegion.amzrNotSpecified);
    end;
  end;
end;

function TAmazonChinaStorageService.GetBucketLoggingXML(const BucketName: string;
                                                   ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  url := GetConnectionInfo.StorageURL(BucketName) + '/?logging=';

  Headers := InitHeaders(BucketName);

  QueryPrefix := '/' + BucketName + '/?logging';

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetBucketLogging(const BucketName: string;
                                                ResponseInfo: TCloudResponseInfo): TAmazonBucketLoggingInfo;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ItemNode, Aux: IXMLNode;
  TargetBucket: string;
begin
  Result := nil;

  xml := GetBucketLoggingXML(BucketName, ResponseInfo);

  if xml = EmptyStr then
    Exit;

  xmlDoc := TXMLDocument.Create(nil);

  try
    xmlDoc.LoadFromXML(xml);
  except
    Exit;
  end;

  RootNode := xmlDoc.DocumentElement;

  if not AnsiSameText(RootNode.NodeName, 'BucketLoggingStatus') then
    Exit;

  try
    if RootNode.HasChildNodes then
    begin
      ItemNode := RootNode.ChildNodes.FindNode('LoggingEnabled');

      if (ItemNode <> nil) and ItemNode.HasChildNodes then
      begin
        Aux := ItemNode.ChildNodes.FindNode('TargetBucket');
        if (Aux <> nil) and Aux.IsTextElement then
        begin
          TargetBucket := Aux.Text;

          Aux := ItemNode.ChildNodes.FindNode('TargetPrefix');
          if (Aux <> nil) and Aux.IsTextElement then
          begin
            Result := TAmazonBucketLoggingInfo.Create(TargetBucket, Aux.Text);

            //Optionally populate Grant information
            Aux := ItemNode.ChildNodes.FindNode('TargetGrants');
            if (Aux <> nil) and Aux.HasChildNodes then
            begin
              PopulateGrants(Aux, Result.Grants);
            end;
          end;
        end;
      end;
    end;
  finally
    //If returning nil, then create a logging info instance with logging disabled
    if Result = nil then
      Result := TAmazonBucketLoggingInfo.Create;
  end;
end;

function TAmazonChinaStorageService.GetBucketNotificationXML(const BucketName: string;
                                                        ResponseInfo: TCloudResponseInfo;
                                                        BucketRegion: TAmazonChinaRegion): string;
var
  url, virtualhost: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  Response := nil;
  try
    Headers := InitHeaders(BucketName);

    if not GetConnectionInfo.UseDefaultEndpoints then
    begin
      virtualhost := BucketName + '.' + GetConnectionInfo.StorageEndpoint;
      url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]) + '/?notification=';
      Headers.Values['host'] := virtualhost;
    end else
    if not GetRegionFromEndpoint(TAmazonChinaServiceType.csS3, GetConnectionInfo.StorageEndpoint).Equals(TAmazonChinaRegions[BucketRegion]) then
    begin
      virtualhost := GetVirtualHostFromRegion(BucketName, BucketRegion);
      url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]) + '/?notification=';
      Headers.Values['host'] := virtualhost;
    end
    else
      url := GetConnectionInfo.StorageURL(BucketName) + '/?notification=';

    QueryPrefix := '/' + BucketName + '/?notification';

    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    Response.Free;
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetBucketNotification(const BucketName: string;
                                      ResponseInfo: TCloudResponseInfo;
                                      BucketRegion: TAmazonChinaRegion): TList<TAmazonNotificationEvent>;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, TopicNode, Aux: IXMLNode;
  TopicStr: string;
begin
  Result := nil;
  xml := GetBucketNotificationXML(BucketName, ResponseInfo, BucketRegion);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit;
    end;

    RootNode := xmlDoc.DocumentElement;

    if AnsiSameText(RootNode.NodeName, 'NotificationConfiguration')  then
    begin
      Result := TList<TAmazonNotificationEvent>.Create;
      if RootNode.HasChildNodes then
      begin
        //Currently there will only be one TopicConfiguration node, but this will
        //allow for gracefully handling the case if/when more TopicConfigurations are allowed.
        TopicNode := RootNode.ChildNodes.First;

        while TopicNode <> nil do
        begin
          if AnsiSameText(TopicNode.NodeName, 'TopicConfiguration') and TopicNode.HasChildNodes then
          begin
            Aux := TopicNode.ChildNodes.FindNode('Topic');
            if (Aux <> nil) and Aux.IsTextElement then
            begin
              TopicStr := Aux.Text;
              Aux := TopicNode.ChildNodes.FindNode('Event');
              if (Aux <> nil) and Aux.IsTextElement then
              begin
                Result.Add(TAmazonNotificationEvent.Create(TopicStr, Aux.Text));
              end;
            end;
          end;
          TopicNode := TopicNode.NextSibling;
        end;
      end;
    end;
  end;
end;

function TAmazonChinaStorageService.GetBucketObjectVersionsXML(const BucketName: string; OptionalParams: TStrings;
                                                          ResponseInfo: TCloudResponseInfo): string;
begin
  Result := GetBucketXMLInternal(BucketName, OptionalParams, True, ResponseInfo);
end;

function TAmazonChinaStorageService.GetBucketObjectVersions(const BucketName: string; OptionalParams: TStrings;
                                                       ResponseInfo: TCloudResponseInfo): TAmazonBucketResult;
var
  xml: string;
begin
  xml := GetBucketObjectVersionsXML(BucketName, OptionalParams, ResponseInfo);
  Result := GetBucketInternal(xml, ResponseInfo);
end;

function TAmazonChinaStorageService.GetRequestPaymentXML(const BucketName: string;
                                                    ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  url := GetConnectionInfo.StorageURL(BucketName) + '/?requestPayment=';

  Headers := InitHeaders(BucketName);

  QueryPrefix := '/' + BucketName + '/?requestPayment';

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetRequestPayment(const BucketName: string;
                                                 ResponseInfo: TCloudResponseInfo): TAmazonPayer;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, AuxNode: IXMLNode;
begin
  Result := ampUnknown;
  xml := GetRequestPaymentXML(BucketName, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit;
    end;

    RootNode := xmlDoc.DocumentElement;

    if AnsiSameText(RootNode.NodeName, 'RequestPaymentConfiguration') and RootNode.HasChildNodes then
    begin
      AuxNode := RootNode.ChildNodes.FindNode('Payer');
      if (AuxNode <> nil) and AuxNode.IsTextElement then
      begin
        if AnsiSameText(AuxNode.Text, 'Requester') then
          Result := ampRequester
        else
          Result := ampBucketOwner;
      end;
    end;
  end;
end;

function TAmazonChinaStorageService.GetBucketVersioningXML(const BucketName: string;
                                                      ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  url := GetConnectionInfo.StorageURL(BucketName) + '/?versioning=';

  Headers := InitHeaders(BucketName);

  QueryPrefix := '/' + BucketName + '/?versioning';

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetBucketVersioning(const BucketName: string;
                                                   ResponseInfo: TCloudResponseInfo): Boolean;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, AuxNode: IXMLNode;
begin
  Result := false;
  xml := GetBucketVersioningXML(BucketName, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit;
    end;

    RootNode := xmlDoc.DocumentElement;

    if AnsiSameText(RootNode.NodeName, 'VersioningConfiguration') and RootNode.HasChildNodes then
    begin
      AuxNode := RootNode.ChildNodes.FindNode('Status');
      if (AuxNode <> nil) and AuxNode.IsTextElement then
        Result := AnsiSameText(AuxNode.Text, 'Enabled');
    end;
  end;
end;

function TAmazonChinaStorageService.GetBucketMFADelete(const BucketName: string;
                                                  ResponseInfo: TCloudResponseInfo): Boolean;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, AuxNode: IXMLNode;
begin
  Result := false;
  xml := GetBucketVersioningXML(BucketName, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit;
    end;

    RootNode := xmlDoc.DocumentElement;

    if AnsiSameText(RootNode.NodeName, 'VersioningConfiguration') and RootNode.HasChildNodes then
    begin
      AuxNode := RootNode.ChildNodes.FindNode('MfaDelete');
      if (AuxNode <> nil) and AuxNode.IsTextElement then
        Result := AnsiSameText(AuxNode.Text, 'Enabled');
    end;
  end;
end;

function TAmazonChinaStorageService.GetBucketLifecycleXML(const ABucketName: string;
  const AResponseInfo: TCloudResponseInfo): string;
var
  LUrl: string;
  LHeaders: TStringList;
  LResponse: TCloudHTTP;
begin
  LResponse := nil;
  LHeaders := nil;
  try
    LUrl := GetConnectionInfo.StorageURL(ABucketName) + '/?lifecycle=';
    LHeaders := InitHeaders(ABucketName);
    LResponse := IssueGetRequest(LUrl, LHeaders, nil, '', AResponseInfo, Result);
    ParseResponseError(AResponseInfo, Result);
  finally
    LResponse.Free;
    LHeaders.Free;
  end;
end;

function TAmazonChinaStorageService.ListMultipartUploadsXML(const BucketName: string;
                                                       const OptionalParams: TStrings;
                                                       ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryParams: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  url := GetConnectionInfo.StorageURL(BucketName) + '/';

  QueryParams := TStringList.Create;
  QueryParams.Values['uploads'] := ' ';

  if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    QueryParams.AddStrings(OptionalParams);

  url := BuildQueryParameterString(url, QueryParams, False, True);

  Headers := InitHeaders(BucketName);

  QueryPrefix := '/' + BucketName + '/?uploads';

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.ListMultipartUploads(const BucketName: string; const OptionalParams: TStrings;
                                             ResponseInfo: TCloudResponseInfo): TAmazonMultipartUploadsResult;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ItemNode, UploadItemNode: IXMLNode;
  Item: TAmazonMultipartUploadItem;
begin
  Result := nil;
  xml := ListMultipartUploadsXML(BucketName, OptionalParams, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit;
    end;

    RootNode := xmlDoc.DocumentElement;

    if AnsiSameText(RootNode.NodeName, 'ListMultipartUploadsResult') and RootNode.HasChildNodes then
    begin
      ItemNode := RootNode.ChildNodes.FindNode('Bucket');
      if (ItemNode = nil) or (not ItemNode.IsTextElement) then
        Exit;

      Result := TAmazonMultipartUploadsResult.Create(ItemNode.Text);

      ItemNode := RootNode.ChildNodes.First;

      while ItemNode <> nil do
      begin
        if AnsiSameText(ItemNode.NodeName, 'KeyMarker') then
          Result.KeyMarker := ItemNode.Text
        else if AnsiSameText(ItemNode.NodeName, 'UploadIdMarker') then
          Result.UploadIdMarker := ItemNode.Text
        else if AnsiSameText(ItemNode.NodeName, 'NextKeyMarker') then
          Result.NextKeyMarker := ItemNode.Text
        else if AnsiSameText(ItemNode.NodeName, 'NextUploadIdMarker') then
          Result.NextUploadIdMarker := ItemNode.Text
        else if AnsiSameText(ItemNode.NodeName, 'MaxUploads') then
          try
            Result.MaxUploads := StrToInt(ItemNode.Text);
          except
          end
        else if AnsiSameText(ItemNode.NodeName, 'IsTruncated') then
          Result.IsTruncated := AnsiSameText(ItemNode.Text, 'true')
        else if AnsiSameText(ItemNode.NodeName, 'Upload') and ItemNode.HasChildNodes then
        begin
          UploadItemNode := ItemNode.ChildNodes.FindNode('Key');
          if (UploadItemNode <> nil) and UploadItemNode.IsTextElement then
          begin
            Item := TAmazonMultipartUploadItem.Create(UploadItemNode.Text);

            UploadItemNode := ItemNode.ChildNodes.First;

            while UploadItemNode <> nil do
            begin
              if AnsiSameText(UploadItemNode.NodeName, 'UploadId') then
                Item.UploadId := UploadItemNode.Text
              else if AnsiSameText(UploadItemNode.NodeName, 'StorageClass') then
                Item.IsReducedRedundancyStorage := AnsiSameText(UploadItemNode.Text, 'REDUCED_REDUDANCY')
              else if AnsiSameText(UploadItemNode.NodeName, 'Initiated') then
                Item.DateInitiated := UploadItemNode.Text
              else if AnsiSameText(UploadItemNode.NodeName, 'Initiator') and UploadItemNode.HasChildNodes then
              begin
                Item.InitiatorId := GetChildText('ID', UploadItemNode);
                Item.InitiatorDisplayName := GetChildText('DisplayName', UploadItemNode);
              end
              else if AnsiSameText(UploadItemNode.NodeName, 'Owner') and UploadItemNode.HasChildNodes then
              begin
                Item.OwnerId := GetChildText('ID', UploadItemNode);
                Item.OwnerDisplayName := GetChildText('DisplayName', UploadItemNode);
              end;

              UploadItemNode := UploadItemNode.NextSibling;
            end;

            Result.UploadItems.Add(Item);
          end;
        end;

        ItemNode := ItemNode.NextSibling;
      end;
    end;
  end;
end;

function TAmazonChinaStorageService.SetBucketACL(const BucketName: string; ACP: TAmazonAccessControlPolicy;
                                            ResponseInfo: TCloudResponseInfo): Boolean;
var
  xml: string;
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  ContentLength: Integer;
  contentStream: TStringStream;
  responseStr: string;
begin
  if (BucketName = EmptyStr) or (ACP = nil) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/?acl=';

  xml := ACP.AsXML;

  contentStream := TStringStream.Create;
  contentStream.WriteString(xml);
  contentStream.position := 0;
  ContentLength := contentStream.Size;

  Headers := InitHeaders(BucketName);
  Headers.Values['Content-Length'] := IntToStr(ContentLength);
  Headers.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';

  if ContentLength > 0 then
     Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(xml);

  QueryPrefix := '/' + BucketName + '/?acl';

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.SetBucketPolicy(const BucketName: string; Policy: TJSONObject;
                                               ResponseInfo: TCloudResponseInfo): Boolean;
var
  policyString: string;
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  ContentLength: Integer;
  contentStream: TStringStream;
  responseStr: string;
begin
  if (BucketName = EmptyStr) or (Policy = nil) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/?policy=';

  policyString := Policy.ToString;

  contentStream := TStringStream.Create;
  contentStream.WriteString(policyString);
  contentStream.position := 0;
  ContentLength := contentStream.Size;

  Headers := InitHeaders(BucketName);
  Headers.Values['Content-Length'] := IntToStr(ContentLength);
  Headers.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';

   if ContentLength > 0 then
     Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(policyString);

  QueryPrefix := '/' + BucketName + '/?policy';

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 204);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.SetBucketLogging(const BucketName: string;
                            LoggingInfo: TAmazonBucketLoggingInfo; ResponseInfo: TCloudResponseInfo): Boolean;
var
  loggingString: string;
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  ContentLength: Integer;
  contentStream: TStringStream;
  responseStr: string;
begin
  if (BucketName = EmptyStr) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/?logging=';

  if LoggingInfo = nil then
    loggingString := TAmazonBucketLoggingInfo.GetDisableXML
  else
    loggingString := LoggingInfo.AsXML;

  contentStream := TStringStream.Create;
  contentStream.WriteString(loggingString);
  contentStream.position := 0;
  ContentLength := contentStream.Size;

  Headers := InitHeaders(BucketName);
  Headers.Values['Content-Length'] := IntToStr(ContentLength);
  Headers.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';

  if ContentLength > 0 then
     Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(loggingString);

  QueryPrefix := '/' + BucketName + '/?logging';

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.SetBucketNotification(const BucketName: string;
                                                     Events: TList<TAmazonNotificationEvent>;
                                                     ResponseInfo: TCloudResponseInfo;
                                                     BucketRegion: TAmazonChinaRegion): Boolean;
var
  xml: string;
  url, virtualhost: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  ContentLength: Integer;
  contentStream: TStringStream;
  responseStr: string;
  location: TAmazonChinaRegion;
begin
  if (BucketName = EmptyStr)then
    Exit(False);

  contentStream := nil;
  Response := nil;
  try
    Headers := InitHeaders(BucketName);
    if BucketRegion = amzrNotSpecified then
    begin
      location := GetBucketLocation(BucketName, ResponseInfo);
      if location = amzrNotSpecified then
        exit(False);
    end
    else
      location := BucketRegion;

    if not GetConnectionInfo.UseDefaultEndpoints then
    begin
      virtualhost := BucketName + '.' + GetConnectionInfo.StorageEndpoint;
      url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]) + '/?notification=';
      Headers.Values['host'] := virtualhost;
    end else
    if not GetRegionFromEndpoint(TAmazonChinaServiceType.csS3, GetConnectionInfo.StorageEndpoint).Equals(TAmazonChinaRegions[BucketRegion]) then
    begin
      virtualhost := GetVirtualHostFromRegion(BucketName, BucketRegion);
      url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]) + '/?notification=';
      Headers.Values['host'] := virtualhost;
    end
    else
      url := GetConnectionInfo.StorageURL(BucketName) + '/?notification=';

    xml := GetNotificationXML(Events);

    contentStream := TStringStream.Create;
    contentStream.WriteString(xml);
    contentStream.position := 0;
    ContentLength := contentStream.Size;

    Headers.Values['Content-Length'] := IntToStr(ContentLength);
    Headers.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';
    if ContentLength > 0 then
       Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(xml);

    QueryPrefix := '/' + BucketName + '/?notification';

    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    Response.Free;
    FreeAndNil(Headers);
    contentStream.Free;
  end;
end;

function TAmazonChinaStorageService.SetBucketRequestPayment(const BucketName: string; Payer: TAmazonPayer;
                                                       ResponseInfo: TCloudResponseInfo): Boolean;
var
  sb: TStringBuilder;
  xml: string;
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  ContentLength: Integer;
  contentStream: TStringStream;
  responseStr: string;
begin
  //must be a valid Payer
  if (BucketName = EmptyStr) or ((Payer <> ampRequester) and (Payer <> ampBucketOwner)) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/?requestPayment=';

  sb := TStringBuilder.Create;

  sb.Append('<RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Payer>');
  if Payer = ampRequester then
    sb.Append('Requester')
  else
    sb.Append('BucketOwner');
  sb.Append('</Payer></RequestPaymentConfiguration>');

  xml := sb.ToString;
  FreeAndNil(sb);

  contentStream := TStringStream.Create;
  contentStream.WriteString(xml);
  contentStream.position := 0;
  ContentLength := contentStream.Size;

  Headers := InitHeaders(BucketName);
  Headers.Values['Content-Length'] := IntToStr(ContentLength);
  Headers.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';
  if ContentLength > 0 then
     Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(xml);

  QueryPrefix := '/' + BucketName + '/?requestPayment';

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.SetBucketVersioning(const BucketName: string; Enabled, MFADelete: Boolean;
                                                   ResponseInfo: TCloudResponseInfo): Boolean;
var
  sb: TStringBuilder;
  xml: string;
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  ContentLength: Integer;
  contentStream: TStringStream;
  responseStr: string;
  MFAInfoAvailable: Boolean;
begin
  if (BucketName = EmptyStr) then
    Exit(False);

  //MFA can only be used when the protocol is HTTPS
  MFAInfoAvailable := (GetConnectionInfo.MFASerialNumber <> EmptyStr) and
                      (GetConnectionInfo.MFAAuthCode <> EmptyStr) and
                       AnsiSameText(GetConnectionInfo.Protocol, 'https');

  //Fail if enabling MFA Delete but no MFA information is specified on the connection.
  if MFADelete and (not MFAInfoAvailable) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/?versioning=';

  sb := TStringBuilder.Create;

  sb.Append('<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>');
  if Enabled then
    sb.Append('Enabled')
  else
    sb.Append('Suspended');
  sb.Append('</Status><MfaDelete>');
  if MFADelete then
    sb.Append('Enabled')
  else
    sb.Append('Disabled');
  sb.Append('</MfaDelete></VersioningConfiguration>');

  xml := sb.ToString;
  FreeAndNil(sb);

  contentStream := TStringStream.Create;
  contentStream.WriteString(xml);
  contentStream.position := 0;
  ContentLength := contentStream.Size;

  Headers := InitHeaders(BucketName);
  Headers.Values['Content-Length'] := IntToStr(ContentLength);
  Headers.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';
  if ContentLength > 0 then
     Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(xml);

  //set the MFA Info if it is available
  if MFAInfoAvailable then
    Headers.Values['x-amz-mfa'] :=
       Format('%s %s', [GetConnectionInfo.MFASerialNumber, GetConnectionInfo.MFAAuthCode]);

  QueryPrefix := '/' + BucketName + '/?versioning';

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.SetBucketLifecycle(const ABucketName: string;
  const ALifeCycle: TAmazonLifeCycleConfiguration; const AResponseInfo: TCloudResponseInfo): boolean;
var
  LContentStream: TStringStream;
  LContentLength: Integer;
  LHeaders: TStringList;
  LUrl, LXml: string;
  LResponse: TCloudHTTP;
begin
  LResponse := nil;
  LHeaders := nil;
  LContentStream := nil;
  try
    LUrl := GetConnectionInfo.StorageURL(ABucketName) + '/?lifecycle=';
    LXml := ALifeCycle.XML;

    LContentStream := TStringStream.Create;
    LContentStream.WriteString(LXml);
    LContentStream.Position := 0;
    LContentLength := LContentStream.Size;

    LHeaders := InitHeaders(ABucketName);
    LHeaders.Values['Content-Length'] := IntToStr(LContentLength);
    LHeaders.Values['Content-MD5'] := TNetEncoding.Base64.EncodeBytesToString(THashMD5.GetHashBytes(LXml));
    if LContentLength > 0 then
      LHeaders.Values['x-amz-content-sha256'] := TCloudSHA256Authentication.GetHashSHA256Hex(LXml);

    LResponse := IssuePutRequest(LUrl, LHeaders, nil, '', AResponseInfo, LContentStream);
    Result := (LResponse <> nil) and (LResponse.ResponseCode = 200);
  finally
    LContentStream.Free;
    LHeaders.Free;
    LResponse.Free;
  end;
end;

function TAmazonChinaStorageService.DeleteObjectInternal(const BucketName, ObjectName, VersionId: string;
                                                    ResponseInfo: TCloudResponseInfo): Boolean;
var
  url: string;
  Headers: TStringList;
  QueryParams: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  RespStr: string;
  MFAInfoAvailable: Boolean;
begin
  QueryParams := nil;

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName;

  if VersionId <> EmptyStr then
  begin
    QueryParams := TStringList.Create;
    QueryParams.Values['versionId'] := VersionId;
    url := BuildQueryParameterString(url, QueryParams, False, True);
  end;

  //MFA can only be used when the protocol is HTTPS and a versioned object is being deleted
  MFAInfoAvailable := (VersionId <> EmptyStr) and
                      (GetConnectionInfo.MFASerialNumber <> EmptyStr) and
                      (GetConnectionInfo.MFAAuthCode <> EmptyStr) and
                       AnsiSameText(GetConnectionInfo.Protocol, 'https');

  Headers := InitHeaders(BucketName);

  //set the MFA Info if it is available
  if MFAInfoAvailable then
    Headers.Values['x-amz-mfa'] :=
       Format('%s %s', [GetConnectionInfo.MFASerialNumber, GetConnectionInfo.MFAAuthCode]);

  QueryPrefix := '/' + BucketName + '/' + ObjectName;

  Response := nil;
  try
    Response := IssueDeleteRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo, RespStr);
    ParseResponseError(ResponseInfo, RespStr);
    Result := (Response <> nil) and (Response.ResponseCode = 204);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaStorageService.DeleteObject(const BucketName, ObjectName: string;
                                            ResponseInfo: TCloudResponseInfo): Boolean;
begin
  Result := DeleteObjectInternal(BucketName, ObjectName, EmptyStr, ResponseInfo);
end;

function TAmazonChinaStorageService.DeleteObjectVersion(const BucketName, ObjectName, VersionId: string;
                                                   ResponseInfo: TCloudResponseInfo): Boolean;
begin
  Result := DeleteObjectInternal(BucketName, ObjectName, VersionId, ResponseInfo);
end;

function TAmazonChinaStorageService.DeleteBucketLifecycle(const ABucketName: string;
  const AResponseInfo: TCloudResponseInfo): Boolean;
var
  LUrl: string;
  LHeaders: TStringList;
  LResponse: TCloudHTTP;
begin
  LResponse := nil;
  LHeaders := nil;
  try
    LUrl := GetConnectionInfo.StorageURL(ABucketName) + '/?lifecycle=';
    LHeaders := InitHeaders(ABucketName);
    LResponse := IssueDeleteRequest(LUrl, LHeaders, nil, '', AResponseInfo);
    Result := (LResponse <> nil) and (LResponse.ResponseCode = 204);
  finally
    LHeaders.Free;
    LResponse.Free;
  end;
end;

function TAmazonChinaStorageService.GetObject(const BucketName, ObjectName: string;
                                         OptionalParams: TAmazonGetObjectOptionals;
                                         ObjectStream: TStream;
                                         ResponseInfo: TCloudResponseInfo): Boolean;
begin
  Result := GetObjectInternal(BucketName, ObjectName, EmptyStr, OptionalParams, ResponseInfo, ObjectStream);
end;

function TAmazonChinaStorageService.GetObject(const BucketName, ObjectName: string;
                                         ObjectStream: TStream;
                                         ResponseInfo: TCloudResponseInfo): Boolean;
begin
  Result := GetObject(BucketName, ObjectName, TAmazonGetObjectOptionals.Create, ObjectStream, ResponseInfo);
end;

function TAmazonChinaStorageService.GetObjectVersion(const BucketName, ObjectName, VersionId: string;
                                                OptionalParams: TAmazonGetObjectOptionals;
                                                ObjectStream: TStream;
                                                ResponseInfo: TCloudResponseInfo): Boolean;
begin
  Result := GetObjectInternal(BucketName, ObjectName, VersionId, OptionalParams, ResponseInfo, ObjectStream);
end;

function TAmazonChinaStorageService.GetObjectVersion(const BucketName, ObjectName, VersionId: string;
                                                ObjectStream: TStream;
                                                ResponseInfo: TCloudResponseInfo): Boolean;
begin
  Result := GetObjectVersion(BucketName, ObjectName, VersionId, TAmazonGetObjectOptionals.Create,
                             ObjectStream, ResponseInfo);
end;

function TAmazonChinaStorageService.GetObjectInternal(const BucketName, ObjectName, VersionId: string;
                                                 OptionalParams: TAmazonGetObjectOptionals;
                                                 ResponseInfo: TCloudResponseInfo;
                                                 ObjectStream: TStream): Boolean;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  if (BucketName = EmptyStr)then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName;

  Headers := InitHeaders(BucketName);
  OptionalParams.PopulateHeaders(Headers);

  QueryPrefix := Format('/%s/%s', [BucketName, ObjectName]);

  if VersionId <> EmptyStr then
  begin
    url := Format('%s?versionId=%s', [url, VersionId]);
    QueryPrefix := Format('%s?versionId=%s', [QueryPrefix, VersionId]);
  end;

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, ObjectStream);

    //A 404 error means that the object was deleted. So in a way the request would also
    //be successful when a 404 response code is returned, but the returned stream wouldn't be valid.
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetObjectACLXML(const BucketName, ObjectName: string;
                                               ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  if (BucketName = EmptyStr) or (ObjectName = EmptyStr) then
    Exit;

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName + '?acl=';

  Headers := InitHeaders(BucketName);

  QueryPrefix := Format('/%s/%s?acl', [BucketName, ObjectName]);

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetObjectACL(const BucketName, ObjectName: string;
                                            ResponseInfo: TCloudResponseInfo): TList<TAmazonGrant>;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ListNode: IXMLNode;
begin
  Result := nil;
  xml := GetObjectACLXML(BucketName, ObjectName, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit(nil);
    end;

    RootNode := xmlDoc.DocumentElement;

    if not AnsiSameText(RootNode.NodeName, 'AccessControlPolicy') then
      Exit(nil);

    Result := TList<TAmazonGrant>.Create;
    ListNode := RootNode.ChildNodes.FindNode('AccessControlList');

    if (ListNode = nil) then
      Exit(Result);

    PopulateGrants(ListNode, Result);
  end;
end;

function TAmazonChinaStorageService.GetObjectTorrent(const BucketName, ObjectName: string; ObjectStream: TStream;
                                                ResponseInfo: TCloudResponseInfo): Boolean;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  if (BucketName = EmptyStr)then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName + '?torrent=';

  Headers := InitHeaders(BucketName);

  QueryPrefix := Format('/%s/%s?torrent', [BucketName, ObjectName]);

  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, ObjectStream);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.GetObjectMetadata(const BucketName, ObjectName: string;
                                                 OptionalParams: TAmazonGetObjectOptionals;
                                                 ResponseInfo: TCloudResponseInfo): TStrings;
var
  Properties: TStrings;
begin
  GetObjectProperties(BucketName, ObjectName, TAmazonGetObjectOptionals.Create,
                      Properties, Result, ResponseInfo);
  FreeAndNil(Properties);
end;

function TAmazonChinaStorageService.GetObjectMetadata(const BucketName, ObjectName: string;
                                                 ResponseInfo: TCloudResponseInfo): TStrings;
begin
  Result := GetObjectMetadata(BucketName, ObjectName, TAmazonGetObjectOptionals.Create, ResponseInfo);
end;

function TAmazonChinaStorageService.GetObjectProperties(const BucketName, ObjectName: string;
                                                   OptionalParams: TAmazonGetObjectOptionals;
                                                   out Properties, Metadata: TStrings;
                                                   ResponseInfo: TCloudResponseInfo): Boolean;
var
  CurrentHeaderName, QueryPrefix, url: string;
  Headers: TStringList;
  Response: TCloudHTTP;
  FreeResponseInfo: Boolean;
  I, Count: Integer;
begin
  Result := False;
  Properties := nil;
  Metadata := nil;

  if (BucketName = EmptyStr) or (ObjectName = EmptyStr) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName;

  Headers := InitHeaders(BucketName);
  try
    OptionalParams.PopulateHeaders(Headers);

    QueryPrefix := string.Format('/%s/%s', [BucketName, ObjectName]);

    FreeResponseInfo := ResponseInfo = nil;
    if FreeResponseInfo then
      ResponseInfo := TCloudResponseInfo.Create;

    Response := nil;
    try
      Response := IssueHeadRequest(url, Headers, nil, QueryPrefix, ResponseInfo);
      if (Response <> nil) and (Response.ResponseCode = 200) then
      begin
        Result := True;

        Properties := TStringList.Create;
        Metadata := TStringList.Create;

        Count := ResponseInfo.Headers.Count;
        for I := 0 to Count - 1 do
        begin
          CurrentHeaderName := ResponseInfo.Headers.Names[I];
          if AnsiStartsText('x-amz-meta-', CurrentHeaderName) then
          begin
            //strip the "x-amz-meta-" prefix from the name of the header,
            //to get the original metadata header name, as entered by the user.
            CurrentHeaderName := CurrentHeaderName.Substring(11);
            Metadata.Values[CurrentHeaderName] := ResponseInfo.Headers.ValueFromIndex[I];
          end
          else
            Properties.Values[CurrentHeaderName] := ResponseInfo.Headers.ValueFromIndex[I];
        end;
      end;
    finally
      Response.Free;
      if FreeResponseInfo then
        ResponseInfo.Free;
    end;
  finally
    Headers.Free;
  end;
end;

function TAmazonChinaStorageService.GetObjectProperties(const BucketName, ObjectName: string; out Properties,
                                                   Metadata: TStrings; ResponseInfo: TCloudResponseInfo): Boolean;
begin
  Result := GetObjectProperties(BucketName, ObjectName, TAmazonGetObjectOptionals.Create,
                                Properties, Metadata, ResponseInfo);
end;

function TAmazonChinaStorageService.UploadObject(const BucketName, ObjectName: string; Content: TArray<Byte>;
                                            ReducedRedundancy: Boolean; Metadata, Headers: TStrings;
                                            ACL: TAmazonACLType; ResponseInfo: TCloudResponseInfo): Boolean;
var
  url: string;
  LHeaders: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  contentStream: TBytesStream;
  responseStr: string;
  ContentLength: Integer;
begin
  if (BucketName = EmptyStr) or (ObjectName = EmptyStr) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName;

  LHeaders := InitHeaders(BucketName);
  //if unspecified amazon sets content-type to 'binary/octet-stream';

  if ReducedRedundancy then
    LHeaders.Values['x-amz-storage-class'] := CLASS_REDUCED_REDUNDANCY;

  LHeaders.Values['x-amz-acl'] := GetACLTypeString(ACL);

  if Headers <> nil then
    AddAndValidateHeaders(LHeaders,Headers);

  AddS3MetadataHeaders(LHeaders, Metadata);

  QueryPrefix := Format('/%s/%s', [BucketName, ObjectName]);

  contentStream := TBytesStream.Create(Content);
  contentStream.position := 0;
  ContentLength := contentStream.Size;

  if ContentLength > 0 then
    LHeaders.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetStreamToHashSHA256Hex(contentStream);
  LHeaders.Values['Content-Length'] := IntToStr(ContentLength);

  Response := nil;
  try
    Response := IssuePutRequest(url, LHeaders, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and ((Response.ResponseCode = 200) or (Response.ResponseCode = 100));
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(LHeaders);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.SetObjectACL(const BucketName, ObjectName: string;
                                            ACP: TAmazonAccessControlPolicy;
                                            Headers: TStrings;
                                            ResponseInfo: TCloudResponseInfo): Boolean;
var
  xml: string;
  url: string;
  LHeaders: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  ContentLength: Integer;
  contentStream: TStringStream;
  responseStr: string;
begin
  if (BucketName = EmptyStr) or (ACP = nil) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName + '?acl=';

  xml := ACP.AsXML;

  contentStream := TStringStream.Create;
  contentStream.WriteString(xml);
  contentStream.position := 0;
  ContentLength := contentStream.Size;

  LHeaders := InitHeaders(BucketName);

  if Headers <> nil then
    LHeaders.AddStrings(Headers);

  LHeaders.Values['Content-Length'] := IntToStr(ContentLength);
  LHeaders.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';
  if ContentLength > 0 then
     LHeaders.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(xml);

  QueryPrefix := '/' + BucketName + '/' + ObjectName + '?acl';

  Response := nil;
  try
    Response := IssuePutRequest(url, LHeaders, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(LHeaders);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.SetObjectACL(const BucketName, ObjectName: string; ACL: TAmazonACLType;
                                            ResponseInfo: TCloudResponseInfo): Boolean;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  responseStr: string;
  contentStream: TStringStream;
begin
  if BucketName.IsEmpty then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName + '?acl=';

  Headers := InitHeaders(BucketName);
  Headers.Values['x-amz-acl'] := GetACLTypeString(ACL);
  Headers.Values['Content-Length'] := '0';

  QueryPrefix := '/' + BucketName + '/' + ObjectName + '?acl';

  contentStream := TStringStream.Create;

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.CopyObject(const DestinationBucket, DestinationObjectName, SourceBucket,
                                          SourceObjectName: string; OptionalParams: TAmazonCopyObjectOptionals;
                                          ResponseInfo: TCloudResponseInfo): Boolean;
begin
  Result := CopyObjectVersion(DestinationBucket, DestinationObjectName, SourceBucket,
                              SourceObjectName, EmptyStr, OptionalParams, ResponseInfo);
end;

function TAmazonChinaStorageService.SetObjectMetadata(const BucketName, ObjectName: string; Metadata: TStrings;
                                                 ResponseInfo: TCloudResponseInfo): Boolean;
var
  Optionals: TAmazonCopyObjectOptionals;
begin
  try
    Optionals := TAmazonCopyObjectOptionals.Create;
    Optionals.CopyMetadata := False;
    Optionals.Metadata.AddStrings(Metadata);

    Result := CopyObject(BucketName, ObjectName, BucketName, ObjectName, Optionals, ResponseInfo);
  finally
    FreeAndNil(Optionals);
  end;
end;

procedure TAmazonChinaStorageService.SortHeaders(const Headers: TStringList);
begin
  if (Headers <> nil) then
  begin
    Headers.CustomSort(CaseSensitiveHyphenCompare);
  end;
end;


// For Amazon, add a '=' between each query parameter name anad value, even if a parameter value is empty
function TAmazonChinaStorageService.BuildQueryParameterString(const QueryPrefix: string; QueryParameters: TStringList;
                                                         DoSort, ForURL: Boolean): string;
var
  Count: Integer;
  I: Integer;
  lastParam, nextParam: string;
  QueryStartChar, QuerySepChar, QueryKeyValueSepChar: Char;
  CurrValue: string;
  CommaVal: string;
begin
  //if there aren't any parameters, just return the prefix
  if (QueryParameters = nil) or (QueryParameters.Count = 0) then
    Exit(QueryPrefix);

  if ForURL then
  begin
    //If the query parameter string is beign created for a URL, then
    //we use the default characters for building the strings, as will be required in a URL
    QueryStartChar := '?';
    QuerySepChar := '&';
    QueryKeyValueSepChar := '=';
  end
  else
  begin
    //otherwise, use the characters as they need to appear in the signed string
    QueryStartChar := FQueryStartChar;
    QuerySepChar := FQueryParamSeparator;
    QueryKeyValueSepChar := FQueryParamKeyValueSeparator;
  end;

  if DoSort and not QueryParameters.Sorted then
    SortQueryParameters(QueryParameters, ForURL);

  Count := QueryParameters.Count;

  lastParam := QueryParameters.Names[0];
  CurrValue := Trim(QueryParameters.ValueFromIndex[0]);

  //URL Encode the firs set of params
  URLEncodeQueryParams(ForURL, lastParam, CurrValue);

  Result := QueryPrefix + QueryStartChar + lastParam + QueryKeyValueSepChar + CurrValue;

  //in the URL, the comma character must be escaped. In the StringToSign, it shouldn't be.
  //this may need to be pulled out into a function which can be overridden by individual Cloud services.
  if ForURL then
    CommaVal := '%2c'
  else
    CommaVal := ',';

  //add the remaining query parameters, if any
  for I := 1 to Count - 1 do
  begin
    nextParam := Trim(QueryParameters.Names[I]);
    CurrValue := QueryParameters.ValueFromIndex[I];

    URLEncodeQueryParams(ForURL, nextParam, CurrValue);

    //match on key name only if the key names are not empty string.
    //if there is a key with no value, it should be formatted as in the else block
    if (lastParam <> EmptyStr) and (AnsiCompareText(lastParam, nextParam) = 0) then
      Result := Result + CommaVal + CurrValue
    else begin
      if (not ForURL) or (nextParam <> EmptyStr) then
        Result := Result + QuerySepChar + nextParam + QueryKeyValueSepChar + CurrValue;
      lastParam := nextParam;
    end;
  end;
end;

function TAmazonChinaStorageService.CopyObjectVersion(const DestinationBucket, DestinationObjectName, SourceBucket,
                                                 SourceObjectName, SourceVersionId: string;
                                                 OptionalParams: TAmazonCopyObjectOptionals;
                                                 ResponseInfo: TCloudResponseInfo): Boolean;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  responseStr: string;
  LSourceBucketName, LDestBucketName: string;
begin
  if (DestinationBucket = EmptyStr) or (DestinationObjectName = EmptyStr) or
     (SourceBucket = EmptyStr) or (SourceObjectName = EmptyStr) then
    Exit(False);

  LSourceBucketName := AnsiLowerCase(SourceBucket);
  LDestBucketName := AnsiLowerCase(DestinationBucket);

  url := GetConnectionInfo.StorageURL(LDestBucketName) + '/' + DestinationObjectName;

  QueryPrefix := Format('/%s/%s', [LDestBucketName, DestinationObjectName]);

  Headers := InitHeaders(LDestBucketName);
  Headers.Values['Content-Length'] := '0';

  if OptionalParams <> nil then
    OptionalParams.PopulateHeaders(Headers);

  if SourceVersionId <> EmptyStr then
    Headers.Values['x-amz-copy-source'] :=
      Format('/%s/%s?versionId=%s', [LSourceBucketName, SourceObjectName, SourceVersionId])
  else
    Headers.Values['x-amz-copy-source'] := Format('/%s/%s', [LSourceBucketName, SourceObjectName]);

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, nil, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.InitiateMultipartUploadXML(const BucketName, ObjectName: string; Metadata,
                                                          Headers: TStrings; ACL: TAmazonACLType;
                                                          ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  LHeaders: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
begin
  if (BucketName = EmptyStr) or (ObjectName = EmptyStr) then
    Exit(EmptyStr);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName + '?uploads=';

  QueryPrefix := Format('/%s/%s?uploads', [BucketName, ObjectName]);

  LHeaders := InitHeaders(BucketName);

  if Headers <> nil then
    LHeaders.AddStrings(Headers);

  AddS3MetadataHeaders(LHeaders, Metadata);

  LHeaders.Values['Content-Length'] := '0';
  LHeaders.Values['x-amz-acl'] := GetACLTypeString(ACL);

  Response := nil;
  try
    Response := IssuePostRequest(url, LHeaders, nil, QueryPrefix, ResponseInfo, nil, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(LHeaders);
  end;
end;

function TAmazonChinaStorageService.InitiateMultipartUpload(const BucketName, ObjectName: string; Metadata,
                                                       Headers: TStrings; ACL: TAmazonACLType;
                                                       ResponseInfo: TCloudResponseInfo): string;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, IDNode: IXMLNode;
begin
  xml := InitiateMultipartUploadXML(BucketName, ObjectName, Metadata, Headers, ACL, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit(EmptyStr);
    end;

    RootNode := xmlDoc.DocumentElement;

    if not AnsiSameText(RootNode.NodeName, 'InitiateMultipartUploadResult') then
      Exit(EmptyStr);

    IDNode := RootNode.ChildNodes.FindNode('UploadId');

    if (IDNode <> nil) and IDNode.IsTextElement then
      Result := IDNode.Text;
  end;
end;

function TAmazonChinaStorageService.AbortMultipartUpload(const BucketName, ObjectName, UploadId: string;
                                                    ResponseInfo: TCloudResponseInfo): Boolean;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  responseStr: string;
begin
  if (BucketName = EmptyStr) or (ObjectName = EmptyStr) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName + '?uploadId=' + UploadId;

  QueryPrefix := Format('/%s/%s?uploadId=%s', [BucketName, ObjectName, UploadId]);

  Headers := InitHeaders(BucketName);

  Response := nil;
  try
    Response := IssueDeleteRequest(url, Headers, nil, QueryPrefix, ResponseInfo, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 204);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.UploadPart(const BucketName, ObjectName, UploadId: string; PartNumber: Integer;
                                          Content: TArray<Byte>; out Part: TAmazonMultipartPart;
                                          const ContentMD5: string;
                                          ResponseInfo: TCloudResponseInfo): Boolean;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  contentStream: TBytesStream;
  responseStr: string;
  FreeResponseInfo: Boolean;
  ContentLength: Int64;
begin
  if (BucketName = EmptyStr) or (ObjectName = EmptyStr) or (UploadId = EmptyStr) or (Content = nil) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName +
          Format('?partNumber=%d&uploadId=%s', [PartNumber, UploadId]);

  Headers := InitHeaders(BucketName);

  QueryPrefix := Format('/%s/%s', [BucketName, ObjectName]);
  QueryPrefix := Format('%s?partNumber=%d&uploadId=%s', [QueryPrefix, PartNumber, UploadId]);

  contentStream := TBytesStream.Create(Content);
  ContentLength := contentStream.Size;
  contentStream.position := 0;
  Headers.Values['Content-Length'] := IntToStr(ContentLength);
  Headers.Values['Content-Type'] := 'application/x-www-form-urlencoded; charset=utf-8';
  if ContentLength > 0 then
     Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetStreamToHashSHA256Hex(contentStream);

  if ContentMD5 <> EmptyStr then
    Headers.Values['Content-MD5'] := ContentMD5;

  FreeResponseInfo := ResponseInfo = nil;
  if FreeResponseInfo then
    ResponseInfo := TCloudResponseInfo.Create;

  Response := nil;
  try
    Response := IssuePutRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);

    if Result then
    begin
      Part.PartNumber := PartNumber;
      Part.ETag := ResponseInfo.Headers.Values['ETag'];
      Part.Size := ContentLength;
      Part.LastModified := ResponseInfo.Headers.Values['Date'];
    end;
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(contentStream);
    if FreeResponseInfo then
      ResponseInfo.Free;
  end;
end;

function TAmazonChinaStorageService.CompleteMultipartUpload(const BucketName, ObjectName, UploadId: string;
                                                       Parts: TList<TAmazonMultipartPart>;
                                                       ResponseInfo: TCloudResponseInfo): Boolean;
var
  xml: string;
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  ContentLength: Int64;
  contentStream: TStringStream;
  responseStr: string;
  sb: TStringBuilder;
  Part: TAmazonMultipartPart;
begin
  if (BucketName = EmptyStr) or (ObjectName = EmptyStr) or (UploadId = EmptyStr) or (Parts = nil) then
    Exit(False);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName + '?uploadId=' + URLEncodeValue(UploadId);

  sb := TStringBuilder.Create;

  sb.Append('<CompleteMultipartUpload>');
  for Part In Parts do
    sb.Append(Part.AsXML);
  sb.Append('</CompleteMultipartUpload>');

  xml := sb.toString;
  FreeAndNil(sb);

  contentStream := TStringStream.Create;
  contentStream.WriteString(xml);
  contentStream.position := 0;
  ContentLength := contentStream.Size;

  Headers := InitHeaders(BucketName);
  Headers.Values['Content-Length'] := IntToStr(ContentLength);
  // When the platform is not Windows, it's necessary to specify that we are sending binary content
  Headers.Values['Content-Type'] := 'application/octet-stream';
  if ContentLength > 0 then
     Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetHashSHA256Hex(xml);

  QueryPrefix := '/' + BucketName + '/' + ObjectName + '?uploadId=' + UploadId;

  Response := nil;
  try
    Response := IssuePostRequest(url, Headers, nil, QueryPrefix, ResponseInfo, contentStream, responseStr);
    ParseResponseError(ResponseInfo, responseStr);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
    FreeAndNil(contentStream);
  end;
end;

function TAmazonChinaStorageService.ListMultipartUploadPartsXML(const BucketName, ObjectName, UploadId: string;
                                                           MaxParts, PartNumberMarker: Integer;
                                                           ResponseInfo: TCloudResponseInfo): string;
var
  url: string;
  Headers: TStringList;
  QueryPrefix: string;
  Response: TCloudHTTP;
  QuerySuffix: string;
begin
  if (BucketName = EmptyStr) or (ObjectName = EmptyStr) or (UploadId = EmptyStr)  then
    Exit(EmptyStr);

  QuerySuffix := '?uploadId=' + UploadId;

  if MaxParts > 0 then
    QuerySuffix := QuerySuffix + '&max-parts=' + IntToStr(MaxParts);

  if PartNumberMarker > 0 then
    QuerySuffix := QuerySuffix + '&part-number-marker=' + IntToStr(PartNumberMarker);

  url := GetConnectionInfo.StorageURL(BucketName) + '/' + ObjectName + QuerySuffix;

  QueryPrefix := Format('/%s/%s%s', [BucketName, ObjectName, QuerySuffix]);

  Headers := InitHeaders(BucketName);
  Response := nil;
  try
    Response := IssueGetRequest(url, Headers, nil, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(Headers);
  end;
end;

function TAmazonChinaStorageService.ListMultipartUploadParts(const BucketName, ObjectName, UploadId: string;
                                                        MaxParts, PartNumberMarker: Integer;
                                                        ResponseInfo: TCloudResponseInfo): TAmazonListPartsResult;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ItemNode: IXMLNode;
  PartNumber: Integer;
  ETag: string;
  Part: TAmazonMultipartPart;
begin
  result := nil;
  xml := ListMultipartUploadPartsXML(BucketName, ObjectName, UploadId, MaxParts,
                                     PartNumberMarker, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);

    try
      xmlDoc.LoadFromXML(xml);
    except
      Exit(nil);
    end;

    RootNode := xmlDoc.DocumentElement;

    if not AnsiSameText(RootNode.NodeName, 'ListPartsResult') then
      Exit(nil);

    Result := TAmazonListPartsResult.Create(BucketName, ObjectName, UploadId);

    ItemNode := RootNode.ChildNodes.First;

    while ItemNode <> nil do
    begin
      if AnsiSameText(ItemNode.NodeName, 'Initiator') then
      begin
        Result.InitiatorId := GetChildText('ID', ItemNode);
        Result.InitiatorDisplayName := GetChildText('DisplayName', ItemNode);
      end
      else if AnsiSameText(ItemNode.NodeName, 'Owner') then
      begin
        Result.OwnerId := GetChildText('ID', ItemNode);
        Result.OwnerDisplayName := GetChildText('DisplayName', ItemNode);
      end
      else if AnsiSameText(ItemNode.NodeName, 'StorageClass') then
        Result.IsReducedRedundancyStorage := AnsiSameText(ItemNode.Text, 'REDUCED_REDUDANCY')
      else if AnsiSameText(ItemNode.NodeName, 'PartNumberMarker') then
        try
          Result.PartNumberMarker := StrToInt(ItemNode.Text)
        except
        end
      else if AnsiSameText(ItemNode.NodeName, 'NextPartNumberMarker') then
        try
          Result.NextPartNumberMarker := StrToInt(ItemNode.Text)
        except
        end
      else if AnsiSameText(ItemNode.NodeName, 'MaxParts') then
        try
          Result.MaxParts := StrToInt(ItemNode.Text)
        except
        end
      else if AnsiSameText(ItemNode.NodeName, 'IsTruncated') then
        Result.IsTruncated := AnsiSameText(ItemNode.Text, 'true')
      else if AnsiSameText(ItemNode.NodeName, 'Part') then
      begin
        try
          PartNumber := StrToInt(GetChildText('PartNumber', ItemNode));
          ETag := GetChildText('ETag', ItemNode);

          Part := TAmazonMultipartPart.Create(PartNumber, ETag);
          Part.LastModified := GetChildText('LastModified', ItemNode);
          Part.Size := StrToInt64(GetChildText('Size', ItemNode));

          Result.Parts.Add(Part);
        except
        end;
      end;

      ItemNode := ItemNode.NextSibling;
    end;
  end;
end;

function TAmazonChinaStorageService.BuildStringToSign(const HTTPVerb: string;
  Headers, QueryParameters: TStringList; const QueryPrefix,
  URL: string): string;
begin
  Result:=BuildStringToSignByService(TAmazonChinaServiceType.csS3, HTTPVerb, Headers,
    QueryParameters, QueryPrefix, URL);
end;




procedure TAmazonChinaStorageService.PrepareRequestSignature(
  const HTTPVerb: string; const Headers, QueryParameters: TStringList;
  const StringToSign: string; var URL: string; Request: TCloudHTTP;
  var Content: TStream);
begin
  PrepareRequestHeaderSignatureByService(TAmazonChinaServiceType.csS3, HTTPVerb, Headers,
    QueryParameters, StringToSign, URL, Request, Content);

end;


end.
