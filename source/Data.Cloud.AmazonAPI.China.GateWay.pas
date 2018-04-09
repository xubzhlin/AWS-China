unit Data.Cloud.AmazonAPI.China.GateWay;
//AWS China API API GateWay 接口
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
  TAmazonChinaGateWayService = class(TAmazonChinaBaseService)
  private
    function InitHeaders: TStringList;
    procedure AddAndValidateHeaders(const defaultHeaders, customHeaders: TStrings);
  protected
    FRequiredHeaderNames:TStrings;
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
    function GetXML(const ApiId, Path:string; OptionalParams: TStringList; ResponseInfo: TCloudResponseInfo):string;
    function HeadXML(const ApiId, Path:string; OptionalParams: TStringList; ResponseInfo: TCloudResponseInfo):string;
    function PostXML(const ApiId, Path:string; OptionalParams: TStringList; Content: string; ResponseInfo: TCloudResponseInfo):string; overload;
    function PostXML(const ApiId, Path:string; OptionalParams: TStringList; Content: TStream; ContentLength: Integer; ResponseInfo: TCloudResponseInfo):string; overload;
    function PutXML(const ApiId, Path:string; OptionalParams: TStringList; Content: string; ResponseInfo: TCloudResponseInfo):string; overload;
    function PutXML(const ApiId, Path:string; OptionalParams: TStringList; Content: TStream; ContentLength: Integer; ResponseInfo: TCloudResponseInfo):string; overload;
    function MergeXML(const ApiId, Path:string; OptionalParams: TStringList; Content: string; ResponseInfo: TCloudResponseInfo):string; overload;
    function MergeXML(const ApiId, Path:string; OptionalParams: TStringList; Content: TStream; ContentLength: Integer; ResponseInfo: TCloudResponseInfo):string; overload;
    function OptionsXML(const ApiId, Path:string; OptionalParams: TStringList; ResponseInfo: TCloudResponseInfo):string;
    function DeleteXML(const ApiId, Path:string; OptionalParams: TStringList; ResponseInfo: TCloudResponseInfo):string;
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

{ TAmazonChinaGateWayService }


procedure TAmazonChinaGateWayService.AddAndValidateHeaders(const defaultHeaders,
  customHeaders: TStrings);
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

function TAmazonChinaGateWayService.BuildQueryParameterString(
  const QueryPrefix: string; QueryParameters: TStringList; DoSort,
  ForURL: Boolean): string;
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

function TAmazonChinaGateWayService.BuildStringToSign(const HTTPVerb: string;
  Headers, QueryParameters: TStringList; const QueryPrefix,
  URL: string): string;
begin
  Result:=BuildStringToSignByService(TAmazonChinaServiceType.csGateWay, HTTPVerb, Headers,
    QueryParameters, QueryPrefix, URL);
end;

function TAmazonChinaGateWayService.BuildStringToSignHeaders(
  Headers: TStringList): string;
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

function TAmazonChinaGateWayService.CreateAuthInstance(
  const ConnectionInfo: TAmazonConnectionInfo): TCloudAuthentication;
begin
  Result := TAmazonAWS4Authentication.Create(ConnectionInfo,True); //GateWay uses HMAC-SHA256
end;

function TAmazonChinaGateWayService.CurrentTime: string;
begin
  Result := FormatDateTime('ddd, dd mmm yyyy hh:nn:ss "GMT"',
                           TTimeZone.Local.ToUniversalTime(Now),
                           TFormatSettings.Create('en-US'));
end;

function TAmazonChinaGateWayService.DeleteXML(const ApiId, Path: string;
  OptionalParams: TStringList; ResponseInfo: TCloudResponseInfo): string;
var
  url, virtualhost: string;
  QueryPrefix: string;
  Headers: TStringList;
  QueryParams: TStringList;
  Response: TCloudHTTP;
begin
  QueryParams := nil;
  Response := nil;
  try
    Headers := InitHeaders;

    virtualhost := Format('%s.%s', [ApiId, GetConnectionInfo.GateWayEndPoint]);
    url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]);
    Headers.Values['host'] := virtualhost;

    if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.AddStrings(OptionalParams);
    end;

    url:=url + Path;

    QueryPrefix:= Path;

    if QueryParams <> nil then
      url := BuildQueryParameterString(url, QueryParams, False, True);

    Response := IssueDeleteRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    QueryParams.Free;
    FreeAndNil(Headers);
  end;

end;

function TAmazonChinaGateWayService.GetCanonicalizedHeaderPrefix: string;
begin
  Result := 'x-amz-';
end;

function TAmazonChinaGateWayService.GetRequiredHeaderNames(
  out InstanceOwner: Boolean): TStrings;
begin
  InstanceOwner := False;
  if (FRequiredHeaderNames = nil)  then
    FRequiredHeaderNames := TStringList.Create;
  if FRequiredHeaderNames.Count = 0 then
  begin
    FRequiredHeaderNames.Add('host');
    FRequiredHeaderNames.Add('x-amz-content-sha256');
    FRequiredHeaderNames.Add('x-amz-date');
  end;
  Result := FRequiredHeaderNames;
end;

function TAmazonChinaGateWayService.GetXML(const ApiId, Path: string; OptionalParams: TStringList;
  ResponseInfo: TCloudResponseInfo): string;
var
  url, virtualhost: string;
  QueryPrefix: string;
  Headers: TStringList;
  QueryParams: TStringList;
  Response: TCloudHTTP;
begin
  QueryParams := nil;
  Response := nil;
  try
    Headers := InitHeaders;

    virtualhost := Format('%s.%s', [ApiId, GetConnectionInfo.GateWayEndPoint]);
    url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]);
    Headers.Values['host'] := virtualhost;

    if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.AddStrings(OptionalParams);
    end;

    url:=url + Path;

    QueryPrefix:= Path;

    if QueryParams <> nil then
      url := BuildQueryParameterString(url, QueryParams, False, True);

    Response := IssueGetRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    QueryParams.Free;
    FreeAndNil(Headers);
  end;

end;

function TAmazonChinaGateWayService.HeadXML(const ApiId, Path: string;
  OptionalParams: TStringList; ResponseInfo: TCloudResponseInfo): string;
var
  url, virtualhost: string;
  QueryPrefix: string;
  Headers: TStringList;
  QueryParams: TStringList;
  Response: TCloudHTTP;
begin
  QueryParams := nil;
  Response := nil;
  try
    Headers := InitHeaders;

    virtualhost := Format('%s.%s', [ApiId, GetConnectionInfo.GateWayEndPoint]);
    url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]);
    Headers.Values['host'] := virtualhost;

    if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.AddStrings(OptionalParams);
    end;

    url:=url + Path;

    QueryPrefix:= Path;

    if QueryParams <> nil then
      url := BuildQueryParameterString(url, QueryParams, False, True);

    Response := IssueHeadRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    QueryParams.Free;
    FreeAndNil(Headers);
  end;


end;

function TAmazonChinaGateWayService.InitHeaders: TStringList;
begin
  Result := TStringList.Create;
  Result.CaseSensitive := false;
  Result.Duplicates := TDuplicates.dupIgnore;
  Result.Values['content-type']:= 'application/json';
  Result.Values['host'] := GetConnectionInfo.GateWayURL;
  Result.Values['x-amz-content-sha256'] := 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'; //empty string
  Result.Values['x-amz-date'] := ISODateTime_noSeparators;
  //Result.Values['x-amz-date'] := '20180301T063019Z';
end;

function TAmazonChinaGateWayService.MergeXML(const ApiId, Path: string;
  OptionalParams: TStringList; Content: string;
  ResponseInfo: TCloudResponseInfo): string;
begin

end;

function TAmazonChinaGateWayService.MergeXML(const ApiId, Path: string;
  OptionalParams: TStringList; Content: TStream; ContentLength: Integer;
  ResponseInfo: TCloudResponseInfo): string;
var
  url, virtualhost: string;
  QueryPrefix: string;
  Headers: TStringList;
  QueryParams: TStringList;
  Response: TCloudHTTP;
begin
  QueryParams := nil;
  Response := nil;
  try
    Headers := InitHeaders;

    virtualhost := Format('%s.%s', [ApiId, GetConnectionInfo.GateWayEndPoint]);
    url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]);
    Headers.Values['host'] := virtualhost;

    if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.AddStrings(OptionalParams);
    end;

    url:=url + Path;

    QueryPrefix:= Path;

    if QueryParams <> nil then
      url := BuildQueryParameterString(url, QueryParams, False, True);

    if ContentLength > 0 then
      Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetStreamToHashSHA256Hex(Content);
    Headers.Values['Content-Length'] := IntToStr(ContentLength);

    Response := IssueMergeRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo, Content);
    Result := Response.ResponseText;
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    QueryParams.Free;
    FreeAndNil(Headers);
  end;


end;

function TAmazonChinaGateWayService.OptionsXML(const ApiId, Path: string;
  OptionalParams: TStringList; ResponseInfo: TCloudResponseInfo): string;
var
  url, virtualhost: string;
  QueryPrefix: string;
  Headers: TStringList;
  QueryParams: TStringList;
  Response: TCloudHTTP;
begin
  QueryParams := nil;
  Response := nil;
  try
    Headers := InitHeaders;

    virtualhost := Format('%s.%s', [ApiId, GetConnectionInfo.GateWayEndPoint]);
    url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]);
    Headers.Values['host'] := virtualhost;

    if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.AddStrings(OptionalParams);
    end;

    url:=url + Path;

    QueryPrefix:= Path;

    if QueryParams <> nil then
      url := BuildQueryParameterString(url, QueryParams, False, True);

    Response := IssueOptionsRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    QueryParams.Free;
    FreeAndNil(Headers);
  end;

end;

procedure TAmazonChinaGateWayService.ParseResponseError(
  const ResponseInfo: TCloudResponseInfo; const ResultString: string);
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

function TAmazonChinaGateWayService.PostXML(const ApiId, Path: string;
  OptionalParams: TStringList; Content: string;
  ResponseInfo: TCloudResponseInfo): string;
var
  ContentStream: TStringStream;
  ContentLength: Integer;
begin
  ContentStream := TStringStream.Create();
  ContentStream.WriteString(Content);
  ContentLength := contentStream.Size;
  ContentStream.position := 0;
  Result := PostXML(ApiId, Path, OptionalParams, ContentStream, ContentLength, ResponseInfo);

end;

function TAmazonChinaGateWayService.PostXML(const ApiId, Path: string;
  OptionalParams: TStringList; Content: TStream; ContentLength: Integer;
  ResponseInfo: TCloudResponseInfo): string;
var
  url, virtualhost: string;
  QueryPrefix: string;
  Headers: TStringList;
  QueryParams: TStringList;
  Response: TCloudHTTP;
begin
  QueryParams := nil;
  Response := nil;
  try
    Headers := InitHeaders;

    virtualhost := Format('%s.%s', [ApiId, GetConnectionInfo.GateWayEndPoint]);
    url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]);
    Headers.Values['host'] := virtualhost;

    if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.AddStrings(OptionalParams);
    end;

    url:=url + Path;

    QueryPrefix:= Path;

    if QueryParams <> nil then
      url := BuildQueryParameterString(url, QueryParams, False, True);

    if ContentLength > 0 then
      Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetStreamToHashSHA256Hex(Content);
    Headers.Values['Content-Length'] := IntToStr(ContentLength);

    Response := IssuePostRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo, Content, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    QueryParams.Free;
    FreeAndNil(Headers);
  end;

end;

procedure TAmazonChinaGateWayService.PrepareRequestSignature(
  const HTTPVerb: string; const Headers, QueryParameters: TStringList;
  const StringToSign: string; var URL: string; Request: TCloudHTTP;
  var Content: TStream);
begin
  PrepareRequestHeaderSignatureByService(TAmazonChinaServiceType.csGateWay, HTTPVerb, Headers,
    QueryParameters, StringToSign, URL, Request, Content);

end;

function TAmazonChinaGateWayService.PutXML(const ApiId, Path: string;
  OptionalParams: TStringList; Content: TStream; ContentLength: Integer;
  ResponseInfo: TCloudResponseInfo): string;
var
  url, virtualhost: string;
  QueryPrefix: string;
  Headers: TStringList;
  QueryParams: TStringList;
  Response: TCloudHTTP;
begin
  QueryParams := nil;
  Response := nil;
  try
    Headers := InitHeaders;

    virtualhost := Format('%s.%s', [ApiId, GetConnectionInfo.GateWayEndPoint]);
    url := Format('%s://%s', [GetConnectionInfo.Protocol, virtualhost]);
    Headers.Values['host'] := virtualhost;

    if (OptionalParams <> nil) and (OptionalParams.Count > 0) then
    begin
      if QueryParams = nil then
        QueryParams := TStringList.Create;
      QueryParams.AddStrings(OptionalParams);
    end;

    url:=url + Path;

    QueryPrefix:= Path;

    if QueryParams <> nil then
      url := BuildQueryParameterString(url, QueryParams, False, True);

    if ContentLength > 0 then
      Headers.Values['x-amz-content-sha256'] :=  TCloudSHA256Authentication.GetStreamToHashSHA256Hex(Content);
    Headers.Values['Content-Length'] := IntToStr(ContentLength);

    Response := IssuePutRequest(url, Headers, QueryParams, QueryPrefix, ResponseInfo, Content, Result);
    ParseResponseError(ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    QueryParams.Free;
    FreeAndNil(Headers);
  end;


end;

function TAmazonChinaGateWayService.PutXML(const ApiId, Path: string;
  OptionalParams: TStringList; Content: string;
  ResponseInfo: TCloudResponseInfo): string;
var
  ContentStream: TStringStream;
  ContentLength: Integer;
begin
  ContentStream := TStringStream.Create;
  ContentStream.WriteString(Content);
  ContentLength := contentStream.Size;
  ContentStream.position := 0;

  Result := PutXML(ApiId, Path, OptionalParams, ContentStream, ContentLength, ResponseInfo);
end;

procedure TAmazonChinaGateWayService.SortHeaders(const Headers: TStringList);
begin
  inherited;

end;

procedure TAmazonChinaGateWayService.URLEncodeQueryParams(const ForURL: Boolean;
  var ParamName, ParamValue: string);
begin
  inherited;

end;

end.
