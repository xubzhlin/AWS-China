unit Data.Cloud.AmazonAPI.China;
//AWS China API 接口
//参考  Data.Cloud.AmazonAPI
//重新完成了 在查询字符串中使用 GET 的身份验证
//目前实现了  S3、SQS、SNS 服务
//测试了部分接口
//有兴趣可以自行测试
//Bug:371889755@qq.com

interface


uses System.Classes,
  System.JSON,
  System.SysUtils,
  System.Generics.Collections,
  Data.Cloud.CloudAPI, Data.Cloud.AmazonAPI.LifeCycle,
  Xml.XMLIntf,
  Data.Cloud.AmazonAPI;

const
  NODE_QUEUES = 'ListQueuesResult';
  NODE_QUEUE = 'QueueUrl';
  NODE_ERRORS = 'Errors';
  NODE_ERROR = 'Error';
  NODE_ERROR_MESSAGE = 'Message';
  NODE_ERROR_CODE = 'Code';
  NODE_REQUEST_ID = 'RequestId';
  NODE_RESPONSE_METADATA = 'ResponseMetadata';
  NODE_QUEUE_CREATE_RESULT = 'CreateQueueResult';
  NODE_QUEUE_ATTRIBS_RESULT = 'GetQueueAttributesResult';
  NODE_ATTRIBUTES = 'Attribute';
  NODE_NAME = 'Name';
  NODE_VALUE = 'Value';
  NODE_QUEUE_MESSAGE_RESULT = 'SendMessageResult';
  NODE_QUEUE_MESSAGE_ID = 'MessageId';
  NODE_QUEUE_MESSAGE_RECEIVE_RESULT = 'ReceiveMessageResult';
  NODE_QUEUE_MESSAGE_MD5 = 'MD5OfBody';
  NODE_QUEUE_MESSAGE_POPRECEIPT = 'ReceiptHandle';
  NODE_QUEUE_MESSAGE_BODY = 'Body';
  NODE_QUEUE_MESSAGE = 'Message';
  CLASS_REDUCED_REDUNDANCY = 'REDUCED_REDUNDANCY';

type
  TAmazonChinaServiceType = (csS3, csSQS, csSNS, csGateWay);

  TAmazonChinaRegion =(amzrNotSpecified, amzrBeijing, amzrNingxia);

  /// <summary>Amazon AWS4 specific implementation of TCloudSHA256Authentication</summary>
  /// <remarks>Sets the Authorization type to 'AWS4-HMAC-SHA256'</remarks>
  TAmazonChinaAWS4Authentication = class(TCloudSHA256Authentication)
  public const
    AuthorizationType = 'AWS4-HMAC-SHA256';
  protected
    procedure AssignKey(const AccountKey: string); override;
  public
    constructor Create(const ConnectionInfo: TCloudConnectionInfo); overload;
    function GetSignatureKey(const datestamp, region, ServiceName: string): TBytes;
    /// <summary>Builds the string to use as the value of the Authorization header of requests.</summary>
    /// <remarks>http://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html</remarks>
    /// <param name="StringToSign">The string to sign and use in the Authorization header value.</param>
    /// <param name="DateISO">Specific date used to build the signature key.</param>
    /// <param name="Region">Specific region used to build the signature key.</param>
    /// <param name="SignedStrHeaders">Signed headers used to build the authorization string.</param>
    function BuildHeaderAuthorizationString(const StringToSign, DateISO, Region, SignedStrHeaders, ServiceName: string): string; reintroduce;
    /// <summary>Builds the string to use as the value of the Authorization Query of requests.</summary>
    /// <remarks>http://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html</remarks>
    /// <param name="StringToSign">The string to sign and use in the Authorization Query value.</param>
    /// <param name="Region">Specific region used to build the signature key.</param>
    /// <param name="ServiceName">Service name used to build the signature key.</param>
    /// <param name="QueryParameters">Query parameters used to build the authorization string.</param>
    function BuildQueryAuthorizationString(const StringToSign, Region, ServiceName: string; const QueryParameters: TStringList): string; reintroduce;
  end;

  TAmazonChinaConnectionInfo = class(TAmazonConnectionInfo)
  private
    FSNSEndpoint: string;
    FGateWayEndPoint:string;
    function GetSNSEndpoint: string;
    function GetSNSURL: string;
    function GetGateWayEndpoint:String;
    function GetGateWayURL:string;
  public
    /// <summary>Creates a new instance of this connection info class</summary>
    /// <param name="AOwner">The component owner.</param>
    constructor Create(AOwner: TComponent); override;
    /// <summary>The sample notify service URL for issuing requests.</summary>
    property SNSURL: string read GetSNSURL;
    property GateWayURL:string read GetGateWayURL;
  published
    /// <summary>The host/endpoint to use when connecting with the SNS  service.</summary>
    property SNSEndpoint: string read GetSNSEndpoint write FSNSEndpoint;
    property GateWayEndPoint:string read GetGateWayEndpoint write FGateWayEndPoint;
  end;

 /// <summary>Implementation of TAmazonService for managing an Amazon Simple Storage Service (S3) account.</summary>
  TAmazonChinaBaseService = class(TAmazonService)
  protected
    function VirtualHost(const ServiceName:TAmazonChinaServiceType; const ResourceName:string = ''):string;
    function InitHeaders(const ServiceName:TAmazonChinaServiceType; const ResourceName:string = ''): TStringList;
    procedure AddAndValidateHeaders(const defaultHeaders,
      customHeaders: TStrings);

    function BuildStringToSignByService(const ServiceName:TAmazonChinaServiceType; const HTTPVerb: string;
      Headers, QueryParameters: TStringList; const QueryPrefix, URL: string): string;

    procedure PrepareRequestHeaderSignatureByService(const ServiceName:TAmazonChinaServiceType; const HTTPVerb: string;
      const Headers, QueryParameters: TStringList; const StringToSign: string;
      var URL: string; Request: TCloudHTTP; var Content: TStream);

    procedure PrepareRequestQuerySignatureByService(const ServiceName:TAmazonChinaServiceType; const HTTPVerb: string;
      const Headers, QueryParameters: TStringList; const StringToSign: string;
      var URL: string; Request: TCloudHTTP; var Content: TStream);
    function GetConnectionInfo:TAmazonChinaConnectionInfo;
  protected
    /// <summary>Returns the list of required header names</summary>
    /// <remarks>Implementation of abstract declaration in parent class.
    ///    Lazy-loads and returns FRequiredHeaderNames. Sets InstanceOwner to false,
    ///    since this class instance will manage the memory for the object.
    /// </remarks>
    /// <param name="InstanceOwner">Returns false, specifying the caller doesn't own the list.</param>
    /// <returns>The list of required hear names. No values.</returns>
    function GetRequiredHeaderNames(out InstanceOwner: Boolean): TStrings; override;
    /// <summary>URL Encodes the param name and value.</summary>
    /// <remarks>Skips encoding if not for URL.</remarks>
    /// <param name="ForURL">True if the parameter is for a URL, false if it is for a signature.</param>
    /// <param name="ParamName">Name of the parameter</param>
    /// <param name="ParamValue">Value of the parameter</param>
    procedure URLEncodeQueryParams(const ForURL: Boolean;
      var ParamName, ParamValue: string); override;
    /// <summary>Returns the authentication instance to use for the given connection info.</summary>
    /// <returns>The authentication instance to use for the given connection info.</returns>
    function CreateAuthInstance(const ConnectionInfo: TAmazonConnectionInfo)
      : TCloudAuthentication; override;
    /// <summary>Returns the header name prefix for Amazon services, for headers to be included
    /// as name/value pairs in the StringToSign in authentication.
    /// </summary>
    /// <returns>The header prefix (x-amz-)</returns>
    function GetCanonicalizedHeaderPrefix: string; override;
    /// <summary>Returns the current time, formatted properly as a string.</summary>
    /// <returns>The current time, formatted properly as a string</returns>
    function CurrentTime: string; override;
    /// <summary>Sorts the given list of Headers.</summary>
    /// <remarks>Extend this if you wish to modify how the list is sorted.</remarks>
    /// <param name="Headers">List of headers to sort.</param>
    procedure SortHeaders(const Headers: TStringList); override;
    /// <summary>Creates the list of query parameters to use for the given action.</summary>
    /// <remarks>Returns the action query parameter, as well as the other parameters common to all requests.
    ///          The action itself should be something like "ListQueues".
    /// </remarks>
    /// <param name="Action">The action currently being performed</param>
    /// <returns></returns>
    function BuildQueryParameters(const ServiceName:TAmazonChinaServiceType; const Action: string): TStringList;
     /// <summary>Sorts the query parameters by name.</summary>
    /// <remarks>This sorts by name, but keeps AWSAccessKeyId at the beginning of the list.</remarks>
    /// <param name="QueryParameters">The list of parameters to sort</param>
    /// <param name="ForURL">True if the parameters are for use in a URL, false otherwise.</param>
    procedure SortQueryParameters(const QueryParameters: TStringList; ForURL: Boolean); override;
    /// <summary>Issues the request, as either a GET or a POST</summary>
    /// <remarks>If the RequestStream is specified, then a POST request is issued and the stream is used
    ///          in the HTTP request as the body and the query parameters are placed in the URL of the request.
    ///          Otherwise, if it is nil, then the CanonicalizedQueryString is built from the given
    ///          information and is placed in the body of the request, which is issued as a GET.
    /// </remarks>
    /// <param name="URL">The request URL, without any query parameters</param>
    /// <param name="QueryParams">The query parameters of the request</param>
    /// <param name="ResponseInfo">The optional response info to populate, or nil</param>
    /// <param name="ResultString">The string representation of the response content.</param>
    /// <param name="RequestStream">The request stream to set as the body of the request, or nil.</param>
    /// <returns>The HTTP request/response object</returns>
    function IssueRequest(URL: string; QueryParams: TStringList; ResponseInfo: TCloudResponseInfo;
                          out ResultString: string; RequestStream: TStream = nil): TCloudHTTP; overload;
    /// <summary>Issues the request, as either a GET or a POST</summary>
    /// <remarks>If the RequestStream is specified, then a POST request is issued and the stream is used
    ///          in the HTTP request as the body and the query parameters are placed in the URL of the request.
    ///          Otherwise, if it is nil, then the CanonicalizedQueryString is built from the given
    ///          information and is placed in the body of the request, which is issued as a GET.
    /// </remarks>
    /// <param name="URL">The request URL, without any query parameters</param>
    /// <param name="QueryParams">The query parameters of the request</param>
    /// <param name="ResponseInfo">The optional response info to populate, or nil</param>
    /// <param name="RequestStream">The request stream to set as the body of the request, or nil.</param>
    /// <returns>The HTTP request/response object</returns>
    function IssueRequest(URL: string; QueryParams: TStringList; ResponseInfo: TCloudResponseInfo;
                          RequestStream: TStream = nil): TCloudHTTP; overload;
     /// <summary>Populates the given ResponseInfo with error information, if any.</summary>
    /// <remarks>If the given ResponseInfo is non-null, and the ResultString holds an XML error message,
    ///          then this procedure will parse the error XML and populate the ResponseInfo's message
    ///          with the error message.
    ///
    ///          This also populates a header value with key 'RequestId', regardless of if the ResultString
    ///          is error XML, or representing a successful request.
    /// </remarks>
    /// <param name="ResponseInfo">The optional response info to populate, or nil</param>
    /// <param name="ResultString">The request's response string to parse for an error message.</param>
    procedure ParseResponseError(const ResponseInfo: TCloudResponseInfo; const ResultString: string);
    /// <summary>Returns the version query parameter value to use in requests.</summary>
    /// <returns>The version query parameter value to use in requests.</returns>
    function GetServiceVersion: string; virtual;
    /// <summary>Returns the host string for the service.</summary>
    /// <returns>The host string for the service.</returns>
    function GetServiceHost: string; virtual;
  public
    /// <summary>Creates a new instance of TAmazonChinaStorageService</summary>
    /// <remarks>This class does not own the ConnectionInfo instance.</remarks>
    // / <param name="ConnectionInfo">The Amazon service connection info</param>
    constructor Create(const ConnectionInfo: TAmazonChinaConnectionInfo);
    /// <summary>Frees the required headers list and destroys the instance</summary>
    destructor Destroy; override;

  end;

const
  TAmazonChinaServiceTypes:array[TAmazonChinaServiceType] of string = ('s3','sqs', 'sns', 'execute-api');
  TAmazonChinaRegions:array[TAmazonChinaRegion] of string = ('cn-north-1', 'cn-north-1', 'cn-northwest-1');


function GetRegionFromEndpoint(const ServiceName:TAmazonChinaServiceType; const endpoint: string): string;

function GetEndpointFromRegion(const ServiceName:TAmazonChinaServiceType; const Region: TAmazonChinaRegion): string;

function GetACLTypeString(BucketACL: TAmazonACLType): string;

function GetRegionFromString(const Region: string): TAmazonChinaRegion;

procedure AddS3MetadataHeaders(Headers, Metadata: TStrings);

function CaseSensitiveHyphenCompare(List: TStringList; Index1, Index2: Integer): Integer;

function CaseSensitiveNameCompare(List: TStringList; Index1, Index2: Integer): Integer;

implementation

uses
  System.Hash, System.NetConsts, System.Net.HTTPClient,
  System.TypInfo, System.DateUtils, Xml.XMLDoc, System.StrUtils,
  System.NetEncoding, System.Net.URLClient
{$IFDEF MACOS}
    , Macapi.CoreFoundation
{$ENDIF MACOS}
    ;

procedure AddS3MetadataHeaders(Headers, Metadata: TStrings);
var
  I, Count: Integer;
  MetaName: string;
begin
  //add the specified metadata into the headers, prefixing each
  //metadata header name with 'x-ms-meta-' if it wasn't already.
  if (MetaData <> nil) and (Headers <> nil) then
  begin
    Count := MetaData.Count;
    for I := 0 to Count - 1 do
    begin
      MetaName := MetaData.Names[I];
      if not AnsiStartsText('x-amz-meta-', MetaName) then
        MetaName := 'x-amz-meta-' + MetaName;
      Headers.Values[MetaName] := MetaData.ValueFromIndex[I];
    end;
  end;
end;

function GetACLTypeString(BucketACL: TAmazonACLType): string;
begin
  case BucketACL of
    amzbaPrivate: Result := 'private';
    amzbaPublicRead: Result := 'public-read';
    amzbaPublicReadWrite: Result := 'public-read-write';
    amzbaAuthenticatedRead: Result := 'authenticated-read';
    amzbaBucketOwnerRead: Result := 'bucket-owner-read';
    amzbaBucketOwnerFullControl: Result := 'bucket-owner-full-control';
    amzbaAWSExecRead: Result := 'aws-exec-read';
    amzbaLogDeliveryWrite: Result := 'log-delivery-write';
  else
    Result := 'private';
  end;
end;

function CaseSensitiveNameCompare(List: TStringList; Index1, Index2: Integer): Integer;
begin
  if List <> nil then
    //CompareStr is used here because Amazon sorts strings based on ASCII order,
    //while AnsiCompareStr does not. (lower-case vs. upper-case)
    Result := CompareStr(List.Names[Index1], List.Names[Index2])
  else
    Result := 0;
end;

function CaseSensitiveHyphenCompare(List: TStringList;
  Index1, Index2: Integer): Integer;
begin
  if List <> nil then
    // case sensitive stringSort is needed to sort with hyphen (-) precedence
    Result := string.Compare(List.Strings[Index1], List.Strings[Index2],
      [coStringSort])
  else
    Result := 0;
end;


function GetEndpointFromRegion(const ServiceName:TAmazonChinaServiceType;
  const Region: TAmazonChinaRegion): string;
begin
  case Region of
    amzrNotSpecified:
      begin
        case ServiceName of
          csS3: Result:='s3.cn-north-1.amazonaws.com.cn';
          csSQS: Result:='sqs.cn-north-1.amazonaws.com.cn';
        end;
      end;
    amzrBeijing:
      begin
        case ServiceName of
          csS3: Result:='s3.cn-north-1.amazonaws.com.cn';
          csSQS: Result:='sqs.cn-north-1.amazonaws.com.cn';
        end;
      end;
    amzrNingxia:
      begin
        case ServiceName of
          csS3: Result:='s3.cn-northwest-1.amazonaws.com.cn';
          csSQS: Result:=''; //宁夏没有SQS服务
        end;
      end;
  end;
end;

function GetRegionFromEndpoint(const ServiceName:TAmazonChinaServiceType;
  const endpoint: string): string;
var
  LStartIdx: Integer;
begin
  if Endpoint.Contains('.amazonaws.com.cn') then
  begin
    if endpoint.Contains('cn-northwest') then
      Exit('cn-northwest');
    if endpoint.Contains('cn-north-1') then
      Exit('cn-north-1')
    else
    begin
      if Endpoint.EndsWith('.amazonaws.com.cn') then
      begin
        if EndPoint.StartsWith(TAmazonChinaServiceTypes[ServiceName]+'.') or EndPoint.StartsWith(TAmazonChinaServiceTypes[ServiceName]+'-') then
          LStartIdx := 3
        else
        begin
          LStartIdx := Pos(TAmazonChinaServiceTypes[ServiceName]+'.', EndPoint);
          if LStartIdx = 0 then
            LStartIdx := Pos(TAmazonChinaServiceTypes[ServiceName]+'-', EndPoint);
          Inc(LStartIdx, 2);
        end;
        Exit(EndPoint.Substring(LStartIdx, EndPoint.Length - LStartIdx - '.amazonaws.com.cn'.Length))
      end
      else
        Exit('cn-north-1');
    end;
  end;
end;

function GetRegionFromString(const Region: string): TAmazonChinaRegion;
begin
  if Region = TAmazonChinaRegions[TAmazonChinaRegion.amzrBeijing] then
    exit(TAmazonChinaRegion.amzrBeijing);
  if Region = TAmazonChinaRegions[TAmazonChinaRegion.amzrNingxia] then
    exit(TAmazonChinaRegion.amzrNingxia)
  else
    exit(TAmazonChinaRegion.amzrNotSpecified);
end;

{ TAmazonChinaBaseService }

procedure TAmazonChinaBaseService.AddAndValidateHeaders(const defaultHeaders,
  customHeaders: TStrings);
var
  IsInstanceOwner: Boolean;
  RequiredHeaders: TStrings;
  I: Integer;
begin
  RequiredHeaders := GetRequiredHeaderNames(IsInstanceOwner);
  for I := 0 to customHeaders.Count - 1 do
  begin
    if not(RequiredHeaders.IndexOfName(customHeaders.Names[I]) > -1) then
      defaultHeaders.Append(customHeaders[I]);
  end;
  if IsInstanceOwner then
    FreeAndNil(RequiredHeaders);
end;

function TAmazonChinaBaseService.BuildQueryParameters(const ServiceName:TAmazonChinaServiceType; const Action: string): TStringList;
var
  LdateISO:string;
  Ldate:string;
  Lregion:string;
  Scope:string;
  AuthorizationString:string;
begin
  Result := TStringList.Create;

  Result.Values['Action'] := Action;
  Result.Values['Version'] := GetServiceVersion;
  Result.Values['X-Amz-Algorithm'] := 'AWS4-HMAC-SHA256';

  LdateISO:=ISODateTime_noSeparators;
  Ldate :=  Leftstr(LdateISO,8);
  Lregion := 'cn-north-1';
  Scope := ConnectionInfo.AccountName + '/' + Ldate + '/'+Lregion+ '/' + TAmazonChinaServiceTypes[ServiceName] + '/aws4_request';
  Result.Values['X-Amz-Credential'] := Scope;

  Result.Values['X-Amz-Date'] := LdateISO;
  Result.Values['X-Amz-Expires'] := '30';
  Result.Values['X-Amz-SignedHeaders'] := 'host';
end;

function TAmazonChinaBaseService.BuildStringToSignByService(
  const ServiceName: TAmazonChinaServiceType; const HTTPVerb: string; Headers,
  QueryParameters: TStringList; const QueryPrefix, URL: string): string;
var
  CanRequest, Scope, LdateISO, Ldate, Lregion: string;
  URLrec: TURI;
  LParams: TStringList;
  VPParam: TNameValuePair;
begin
  LParams := nil;
  try
    // Build the first part of the string to sign, including HTTPMethod
    CanRequest := BuildStringToSignPrefix(HTTPVerb);

    // find and encode the requests resource
    URLrec := TURI.Create(URL);

    // CanonicalURI URL encoded
    CanRequest := CanRequest + URLrec.Path + #10;

    // CanonicalQueryString encoded
    if not URLrec.Query.IsEmpty then
    begin
      if Length(URLrec.Params) = 1 then
        CanRequest := CanRequest + URLrec.Query + #10
      else
      begin
        LParams := TStringList.Create;
        for VPParam in URLrec.Params do
          LParams.Append(VPParam.Name + '=' + VPParam.Value);
        CanRequest := CanRequest + BuildStringToSignResources('', LParams)
          .Substring(1) + #10
      end;
    end
    else
      CanRequest := CanRequest + #10;

    // add sorted headers and header names in series for signedheader part
    CanRequest := CanRequest + BuildStringToSignHeaders(Headers) + #10;

    CanRequest := CanRequest + Headers.Values['x-amz-content-sha256'];
    LdateISO := Headers.Values['x-amz-date'];
    Ldate := LeftStr(LdateISO, 8);
    Lregion := GetRegionFromEndpoint(ServiceName, Headers.Values['Host']);
    Scope := Ldate + '/' + Lregion + '/' + TAmazonChinaServiceTypes[ServiceName] + '/aws4_request';

    Result := 'AWS4-HMAC-SHA256' + #10 + LdateISO + #10 + Scope + #10 +
      TCloudSHA256Authentication.GetHashSHA256Hex(CanRequest);
  finally
    LParams.Free;
  end;

end;


constructor TAmazonChinaBaseService.Create(
  const ConnectionInfo: TAmazonChinaConnectionInfo);
var
  InstanceOwner:Boolean;
  RequiredHeaderNames:TStrings;
begin
  inherited Create(ConnectionInfo);

  FUseCanonicalizedHeaders := True;
  FUseResourcePath := True;

  RequiredHeaderNames:=GetRequiredHeaderNames(InstanceOwner);
  if InstanceOwner then
    FreeAndNil(RequiredHeaderNames);
end;

function TAmazonChinaBaseService.CreateAuthInstance(
  const ConnectionInfo: TAmazonConnectionInfo): TCloudAuthentication;
begin
  Result := TAmazonChinaAWS4Authentication.Create(ConnectionInfo);
  //uses HMAC-SHA256
end;

function TAmazonChinaBaseService.CurrentTime: string;
begin
  Result := FormatDateTime('ddd, dd mmm yyyy hh:nn:ss "GMT"',
    TTimeZone.Local.ToUniversalTime(Now), TFormatSettings.Create('en-US'));
end;

destructor TAmazonChinaBaseService.Destroy;
begin
  inherited;
end;

function TAmazonChinaBaseService.GetCanonicalizedHeaderPrefix: string;
begin
  Result := 'x-amz-';
end;

function TAmazonChinaBaseService.GetConnectionInfo: TAmazonChinaConnectionInfo;
begin
  Result:=TAmazonChinaConnectionInfo(ConnectionInfo);
end;

function TAmazonChinaBaseService.GetRequiredHeaderNames(
  out InstanceOwner: Boolean): TStrings;
begin
  InstanceOwner := False;
  Result := nil;
end;

function TAmazonChinaBaseService.GetServiceHost: string;
begin
  Result := '';
end;

function TAmazonChinaBaseService.GetServiceVersion: string;
begin
  Result := '';
end;

function TAmazonChinaBaseService.InitHeaders(
  const ServiceName: TAmazonChinaServiceType; const ResourceName:string = ''): TStringList;
begin
  Result := TStringList.Create;
  Result.CaseSensitive := False;
  Result.Duplicates := TDuplicates.dupIgnore;
  Result.Values['host'] := VirtualHost(ServiceName, ResourceName);
  Result.Values['x-amz-content-sha256'] :=
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
  // empty string
  Result.Values['x-amz-date'] := ISODateTime_noSeparators;
end;

procedure TAmazonChinaBaseService.PrepareRequestHeaderSignatureByService(
  const ServiceName: TAmazonChinaServiceType; const HTTPVerb: string;
  const Headers, QueryParameters: TStringList; const StringToSign: string;
  var URL: string; Request: TCloudHTTP; var Content: TStream);
var
  AuthorizationString, SignStrHeaders, LdateISO, Lregion: string;
  RequiredHeadersInstanceOwner: Boolean;
  SignedHeaders: TStringList;
begin
  if FAuthenticator <> nil then
  begin
    SignedHeaders := TStringList
      (GetRequiredHeaderNames(RequiredHeadersInstanceOwner));
    SignedHeaders.Delimiter := ';';
    SignStrHeaders := SignedHeaders.DelimitedText;
    LdateISO := LeftStr(Headers.Values['x-amz-date'], 8);
    Lregion := GetRegionFromEndpoint(ServiceName, Headers.Values['Host']);
    AuthorizationString := TAmazonChinaAWS4Authentication(FAuthenticator)
      .BuildHeaderAuthorizationString(StringToSign, LdateISO, Lregion,
      SignStrHeaders, TAmazonChinaServiceTypes[ServiceName]);
    Request.Client.customHeaders['Authorization'] := AuthorizationString;
    SignedHeaders.Clear;
    if RequiredHeadersInstanceOwner then
      FreeAndNil(SignedHeaders);
  end;

end;

procedure TAmazonChinaBaseService.PrepareRequestQuerySignatureByService(const ServiceName:TAmazonChinaServiceType; const HTTPVerb: string;
  const Headers, QueryParameters: TStringList; const StringToSign: string;
  var URL: string; Request: TCloudHTTP; var Content: TStream);
var
  LDateISO, AuthorizationString, QueryString: string;
  SigningKey:TBytes;
begin
  if FAuthenticator <> nil then
  begin
    LDateISO := Leftstr(QueryParameters.Values['X-Amz-Date'], 8);
    SigningKey := TAmazonChinaAWS4Authentication(FAuthenticator).GetSignatureKey(LDateISO, GetRegionFromEndpoint(ServiceName, URL), TAmazonChinaServiceTypes[ServiceName]);

    AuthorizationString := THash.DigestAsString(TAmazonChinaAWS4Authentication(FAuthenticator).SignString(SigningKey, StringToSign));
    QueryParameters.Values['X-Amz-Signature'] := AuthorizationString;

    //If this is a GET request, or the content stream is currently in use by actual request data,
    // then the parameters are in the URL, so add Signature to the URL
    if (HTTPVerb = 'GET') or (Content <> nil) then
      URL := BuildQueryParameterString(URL, QueryParameters, False, True)
    //Otherwise, this is a POST and the parameters should all be put in the content of the request
    else
    begin
      Request.Client.ContentType := 'application/x-www-form-urlencoded; charset=utf-8';

      //Skip the first character, because it is the query prefix character (?) which isn't used
      //when the query string is in the body of the request.
      QueryString := BuildQueryParameterString(EmptyStr, QueryParameters, False, True).Substring(1);
      Content := TStringStream.Create(QueryString);
    end;
  end;
end;

procedure TAmazonChinaBaseService.SortHeaders(const Headers: TStringList);
begin
  if (Headers <> nil) then
  begin
    Headers.CustomSort(CaseSensitiveHyphenCompare);
  end;
end;

procedure TAmazonChinaBaseService.URLEncodeQueryParams(const ForURL: Boolean;
  var ParamName, ParamValue: string);
begin
  ParamName := URLEncodeValue(ParamName);
  ParamValue := URLEncodeValue(ParamValue);
end;

function TAmazonChinaBaseService.VirtualHost(const ServiceName:TAmazonChinaServiceType; const ResourceName:string = ''): string;
begin
  if ServiceName = TAmazonChinaServiceType.csS3 then
  begin
    if ResourceName = EmptyStr then
      Result := GetConnectionInfo.StorageEndpoint
    else
      Result := Format('%s.%s', [ResourceName, GetConnectionInfo.StorageEndpoint]);
  end
  else
  if ServiceName = TAmazonChinaServiceType.csSQS then
    Result:=GetConnectionInfo.QueueEndpoint;
end;

function TAmazonChinaBaseService.IssueRequest(URL: string; QueryParams: TStringList; ResponseInfo: TCloudResponseInfo;
                                          out ResultString: string; RequestStream: TStream): TCloudHTTP;
var
  DoAsPost: Boolean;
  ContentStream: TStream;
begin
  ContentStream := RequestStream;
  DoAsPost := RequestStream <> nil;

  try
    if DoAsPost then
      Result := IssuePostRequest(URL, nil, QueryParams, EmptyStr, ResponseInfo, ContentStream, ResultString)
    else
      Result := IssueGetRequest(URL, nil, QueryParams, EmptyStr, ResponseInfo, ResultString);
  finally
    ParseResponseError(ResponseInfo, ResultString);
  end;
end;

function TAmazonChinaBaseService.IssueRequest(URL: string; QueryParams: TStringList; ResponseInfo: TCloudResponseInfo;
                                          RequestStream: TStream): TCloudHTTP;
var
  OutStr: string;
begin
  Result := IssueRequest(URL, QueryParams, ResponseInfo, OutStr, RequestStream);
end;

procedure TAmazonChinaBaseService.ParseResponseError(const ResponseInfo: TCloudResponseInfo; const ResultString: string);
var
  xmlDoc: IXMLDocument;
  Aux, ErrorNode, MessageNode: IXMLNode;
  ErrorCode, ErrorMsg: string;
  IsErrors: Boolean;
begin
  //If the ResponseInfo instance exists (to be populated) and the passed in string is Error XML, then
  //continue, otherwise exit doing nothing.
  if (ResponseInfo = nil) or (ResultString = EmptyStr) then
    Exit;

  xmlDoc := TXMLDocument.Create(nil);
  xmlDoc.LoadFromXML(ResultString);

  IsErrors := AnsiPos(Format('<%s>', [NODE_ERRORS]), ResultString) > 0;

  //Parse the error and update the ResponseInfo StatusMessage
  if IsErrors or (AnsiPos('<ErrorResponse', ResultString) > 0) then
  begin
    //Amazon has different formats for returning errors as XML
    if IsErrors then
    begin
      ErrorNode := xmlDoc.DocumentElement.ChildNodes.FindNode(NODE_ERRORS);
      ErrorNode := ErrorNode.ChildNodes.FindNode(NODE_ERROR);
    end
    else
      ErrorNode := xmlDoc.DocumentElement.ChildNodes.FindNode(NODE_ERROR);

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

    //populate the RequestId, which is structured differently than if this is not an error response
    Aux := xmlDoc.DocumentElement.ChildNodes.FindNode(NODE_REQUEST_ID);
    if (Aux <> nil) and (Aux.IsTextElement) then
      ResponseInfo.Headers.Values[NODE_REQUEST_ID] := Aux.Text;
  end
  //Otherwise, it isn't an error, but try to pase the RequestId anyway.
  else
  begin
    Aux := xmlDoc.DocumentElement.ChildNodes.FindNode(NODE_RESPONSE_METADATA);
    if Aux <> nil then
    begin
      Aux := Aux.ChildNodes.FindNode(NODE_REQUEST_ID);
      if Aux <> nil then
        ResponseInfo.Headers.Values[NODE_REQUEST_ID] := Aux.Text;
    end;
  end;
end;

procedure TAmazonChinaBaseService.SortQueryParameters(const QueryParameters: TStringList; ForURL: Boolean);
begin
  if (QueryParameters <> nil) and (not ForURL) then
  begin
    QueryParameters.CustomSort(CaseSensitiveNameCompare);
  end;
end;

{ TAmazonChinaAWS4Authentication }

procedure TAmazonChinaAWS4Authentication.AssignKey(const AccountKey: string);
begin
  FSHAKey := TEncoding.Default.GetBytes('AWS4'+FConnectionInfo.AccountKey);
end;

function TAmazonChinaAWS4Authentication.BuildHeaderAuthorizationString(
  const StringToSign, DateISO, Region, SignedStrHeaders,
  ServiceName: string): string;
var
  Signature, Credentials, SignedHeaders: string;
  SigningKey : TBytes;
begin

  SigningKey := GetSignatureKey(DateISO, Region, ServiceName);
  Credentials   := 'Credential='+FConnectionInfo.AccountName + '/'+ DateISO + '/'+Region+ '/' + ServiceName + '/aws4_request'+',';
  SignedHeaders := 'SignedHeaders='+SignedStrHeaders + ',';
  Signature     := 'Signature='+THash.DigestAsString(SignString(SigningKey, StringToSign));
  Result :=  GetAuthorizationType +' '+ Credentials + SignedHeaders + Signature;

end;

function TAmazonChinaAWS4Authentication.BuildQueryAuthorizationString(
  const StringToSign, Region, ServiceName: string;
  const QueryParameters: TStringList): string;
var
  LDateISO, Ldate:string;
  Signature, Credentials, SignedHeaders: string;

begin
  if( QueryParameters = nil) or (QueryParameters.Count = 0) then
    Exit(THash.DigestAsString(SignString(FSHAKey, StringToSign)));

  Signature:=TCloudSHA256Authentication.GetHashSHA256Hex(StringToSign);
  LDateISO := QueryParameters.Values['X-Amz-Date'];
  Ldate :=  Leftstr(LdateISO,8);

  Result := QueryParameters.Values['X-Amz-Algorithm'] + #10 + LdateISO
    + #10 +Ldate + '/' + Region + '/' + ServiceName + '/aws4_request';

  Result := Result + #10 + Signature;

end;

constructor TAmazonChinaAWS4Authentication.Create(
  const ConnectionInfo: TCloudConnectionInfo);
begin
  inherited Create(ConnectionInfo, AuthorizationType);
end;

function TAmazonChinaAWS4Authentication.GetSignatureKey(const datestamp, region,
  ServiceName: string): TBytes;
begin
  Result := SignString(FSHAKey,datestamp); //'20130524'
  Result := SignString(Result, region);
  Result := SignString(Result, ServiceName);
  Result := SignString(Result, 'aws4_request');
end;

{ TAmazonChinaConnectionInfo }

constructor TAmazonChinaConnectionInfo.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);
  FSNSEndpoint := 'sns.amazonaws.com';
  FGateWayEndPoint := 'execute-api.amazonaws.com';
end;

function TAmazonChinaConnectionInfo.GetGateWayEndpoint: String;
begin
  if UseDefaultEndpoints then
    Exit('execute-api.amazonaws.com');

  Exit(FGateWayEndpoint);
end;

function TAmazonChinaConnectionInfo.GetGateWayURL: string;
begin
  Result := Format('%s://%s', [Protocol, FGateWayEndpoint]);
end;

function TAmazonChinaConnectionInfo.GetSNSEndpoint: string;
begin
  if UseDefaultEndpoints then
    Exit('sns.amazonaws.com');

  Exit(FSNSEndpoint);
end;

function TAmazonChinaConnectionInfo.GetSNSURL: string;
begin
  Result := Format('%s://%s', [Protocol, FSNSEndpoint]);
end;


end.
