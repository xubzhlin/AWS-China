unit Data.Cloud.AmazonAPI.China.SNS;
//AWS China API SNS 接口
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
  TAmazonChinaSNSService = class(TAmazonChinaBaseService)
  private

  protected
    /// <summary>Returns the version sample notify service parameter value to use in requests.</summary>
    /// <returns>The version query parameter value to use in requests.</returns>
    function GetServiceVersion: string; override;
    /// <summary>Returns the host string for the service.</summary>
    /// <returns>The host string for the service.</returns>
    function GetServiceHost: string; override;
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
    /// <summary>Builds the beginning part of the StringToSign</summary>
    /// <remarks>The overrides the base implementation, adding "ValueOfHostHeaderInLowercase"
    ///          on a new line after the HTTP verb.
    /// </remarks>
    /// <param name="HTTPVerb">The HTTP verb of the request being made</param>
    /// <returns>The first portion of the StringToSign, ending with a newline character.</returns>
    function BuildStringToSign(const HTTPVerb: string; Headers, QueryParameters: TStringList;
                              const QueryPrefix, URL: string): string; override;
  public
    function AddPublish(const TopicArn, TargetArn, PhoneNumber: string; const MessageText, Subject: string;
                        out MessageId: string; ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
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

{ TAmazonChinaSNSService }

function TAmazonChinaSNSService.AddPublish(const TopicArn, TargetArn,
  PhoneNumber, MessageText, Subject: string; out MessageId: string;
  ResponseInfo: TCloudResponseInfo): Boolean;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ResultNode, IdNode: IXMLNode;
begin

  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSNS, 'Publish');

  QueryParams.Values['TopicArn'] := TopicArn;
  QueryParams.Values['TargetArn'] := TargetArn;
  QueryParams.Values['PhoneNumber'] := PhoneNumber;
  QueryParams.Values['Message'] := MessageText;
  QueryParams.Values['Subject'] := Subject;

  Response := nil;
  try
    Response := IssueRequest(GetConnectionInfo.SNSURL, QueryParams, ResponseInfo, xml);
    Result := (Response <> nil) and (Response.ResponseCode = 200);

    //parse the XML to get the MessageId
    if Result and (xml <> EmptyStr) then
    begin
      xmlDoc := TXMLDocument.Create(nil);
      xmlDoc.LoadFromXML(xml);
      RootNode := xmlDoc.DocumentElement;

      if (RootNode <> nil) and (RootNode.HasChildNodes) then
      begin
        ResultNode := RootNode.ChildNodes.FindNode(NODE_QUEUE_MESSAGE_RESULT);
        if (ResultNode <> nil) and (ResultNode.HasChildNodes) then
        begin
          IdNode := ResultNode.ChildNodes.FindNode(NODE_QUEUE_MESSAGE_ID);
          if (IdNode <> nil) and IdNode.IsTextElement then
            MessageId := IdNode.Text;
        end;
      end;
    end;
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;

end;

function TAmazonChinaSNSService.BuildStringToSign(const HTTPVerb: string;
  Headers, QueryParameters: TStringList; const QueryPrefix,
  URL: string): string;
var
  LdateISO, Ldate, Lregion, Scope:string;
begin
  Lregion := GetRegionFromEndpoint(TAmazonChinaServiceType.csSNS, URL);

  Result:= inherited BuildStringToSign(HTTPVerb, Headers, QueryParameters, QueryPrefix, URL);
  //Host
  Result := Result + #10'host:'+GetServiceHost+#10;
  Result := Result + #10'host';
  Result := Result + #10'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';


  Result := TAmazonChinaAWS4Authentication(FAuthenticator).BuildQueryAuthorizationString(Result, Lregion, 'sns', QueryParameters);

end;

function TAmazonChinaSNSService.GetServiceHost: string;
begin
  Result := GetConnectionInfo.SNSEndpoint;
end;

function TAmazonChinaSNSService.GetServiceVersion: string;
begin
  Result:= '2010-03-31';
end;

procedure TAmazonChinaSNSService.PrepareRequestSignature(const HTTPVerb: string;
  const Headers, QueryParameters: TStringList; const StringToSign: string;
  var URL: string; Request: TCloudHTTP; var Content: TStream);
begin
  PrepareRequestQuerySignatureByService(TAmazonChinaServiceType.csSNS, HTTPVerb, Headers,
    QueryParameters, StringToSign, URL, Request, Content);

end;

end.
