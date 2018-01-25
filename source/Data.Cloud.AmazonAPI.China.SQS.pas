unit Data.Cloud.AmazonAPI.China.SQS;
//AWS China API SQS 接口
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
  /// <summary>Implementation of TAmazonBasicService for managing an Amazon Queue Service account.</summary>
  TAmazonChinaQueueService = class(TAmazonChinaBaseService)
  private
    function IsUniqueMessageId(const MessageId: string;
                               const MessageList: TList<TCloudQueueMessage>): Boolean;
  protected
    /// <summary>Returns the version query parameter value to use in requests.</summary>
    /// <returns>The version query parameter value to use in requests.</returns>
    function GetServiceVersion: string; override;
    /// <summary>Returns the Amazon name for the given attribute</summary>
    /// <param name="Attribute">The attribute to get the amazon parameter name for</param>
    /// <returns>The Amazon parameter name representation of the attribute</returns>
    function GetQueueAttributeName(const Attribute: TAmazonQueueAttribute): string;
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
    /// <summary>Returns the maximum number of queue messages that can be returned.</summary>
    /// <returns>The number of queue messages which can be returned by the API for a given queue.</returns>
    function GetMaxMessageReturnCount: Integer;
    /// <summary>Lists the queues currently available in the queue service account.</summary>
    /// <remarks>The supported optional parameters are: QueueNamePrefix
    ///          The 'QueueNamePrefix' parameter has a value which is the prefix a queue name must start with
    ///          in order to be includes in the list of queues returned by this request.
    /// </remarks>
    /// <param name="OptionalParams">Optional parameters to use in the query. See remarks for more information.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML string representing the queues</returns>
    function ListQueuesXML(OptionalParams: TStrings = nil; ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Lists the queues currently available in the queue service account.</summary>
    /// <remarks>The supported optional parameters are: QueueNamePrefix
    ///          The 'QueueNamePrefix' parameter has a value which is the prefix a queue name must start with
    ///          in order to be includes in the list of queues returned by this request.
    /// </remarks>
    /// <param name="OptionalParams">Optional parameters to use in the query. See remarks for more information.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The list of queues, or an empty list</returns>
    function ListQueues(OptionalParams: TStrings = nil; ResponseInfo: TCloudResponseInfo = nil): TList<TCloudQueue>;
    /// <summary>Creates a queue with the given name.</summary>
    /// <remarks>If DefaultVisibilityTimeout is set to -1, then the service default of 30 seconds is used.
    /// </remarks>
    /// <param name="QueueName">The name of the queue to create.</param>
    /// <param name="DefaultVisibilityTimeout">The visibility timeout (in seconds) to use for this queue.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the create was successful, false otherwise.</returns>
    function CreateQueue(const QueueName: string; const DefaultVisibilityTimeout: Integer = -1;
                         ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Creates a queue with the given name.</summary>
    /// <remarks>If the call is successful, it sets the value of QueueURL to the newly created Queue's URL.
    ///          If DefaultVisibilityTimeout is set to -1, then the service default of 30 seconds is used.
    ///          For the name: max 80 characters; alphanumeric, hyphens, and underscores are allowed.
    /// </remarks>
    /// <param name="QueueName">The name of the queue to create.</param>
    /// <param name="QueueURL">The resulting queue's URL, or empty string is the request fails</param>
    /// <param name="DefaultVisibilityTimeout">The visibility timeout (in seconds) to use for this queue.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the create was successful, false otherwise.</returns>
    function CreateQueue(const QueueName: string; out QueueURL: string; const DefaultVisibilityTimeout: Integer = -1;
                         ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Creates a queue with the given name.</summary>
    /// <remarks>If the call is successful, it sets the value of QueueURL to the newly created Queue's URL.
    ///          For the name: max 80 characters; alphanumeric, hyphens, and underscores are allowed.
    /// </remarks>
    /// <param name="QueueName">The name of the queue to create.</param>
    /// <param name="QueueURL">The resulting queue's URL, or empty string is the request fails</param>
    /// <param name="Attributes">The list of names and values of the special request parameters the CreateQueue action uses.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the create was successful, false otherwise.</returns>
    function CreateQueue(const QueueName: string; out QueueURL: string; Attributes: TArray<TPair<TAmazonQueueAttribute, string>>;
                         ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;

    /// <summary>Deletes the queue with the given URL.</summary>
    /// <remarks>The queue is marked for delete and won't show up in queries, but there will be a short time
    ///          before the server allows another queue with the same name to be created again.
    ///          Note that you must specify the Queue URL, and not just the queue name when deleting.
    ///          The queue URL is provided when calling ListQueues, and is a URL ending in the queue's name.
    /// </remarks>
    /// <param name="QueueURL">The URL of the queue to delete.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the delete is successful, false otherwise</returns>
    function DeleteQueue(const QueueURL: string; ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Returns one or all properties for the specified Queue.</summary>
    /// <remarks>The attribute specified will be the property returned by this call. If you specify 'All'
    ///          as the attribute to return, then all properties of the queue will be returned.
    /// </remarks>
    /// <param name="QueueURL">The URL of the Queue to get the attributes for</param>
    /// <param name="Attribute">The attribute (or All) to get</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>Returns either one or all properties of the given queue.</returns>
    function GetQueuePropertiesXML(const QueueURL: string; Attribute: TAmazonQueueAttribute = aqaAll;
                                   ResponseInfo: TCloudResponseInfo = nil): string; overload;
    /// <summary>Returns one or more properties for the specified Queue.</summary>
    /// <remarks>The attributes specified will be the property returned by this call. If you specify 'All'
    ///          as an attribute to return, then all properties of the queue will be returned.
    ///          If you pass an empty array, then all attributes will be returned.
    /// </remarks>
    // / <param name="QueueURL">The URL of the Queue to get the attributes for</param>
    // / <param name="Attributes">The attributes (or All) to get</param>
    // / <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>Returns one or more properties of the given queue.</returns>
    function GetQueuePropertiesXML(const QueueURL: string; Attributes: array of TAmazonQueueAttribute;
                                   ResponseInfo: TCloudResponseInfo = nil): string; overload;
    /// <summary>Returns one or all properties for the specified Queue.</summary>
    /// <remarks>The attribute specified will be the property returned by this call. If you specify 'All'
    ///          as the attribute to return, then all properties of the queue will be returned.
    /// </remarks>
    /// <param name="QueueURL">The URL of the Queue to get the attributes for</param>
    /// <param name="Attribute">The attribute (or All) to get</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>Returns either one or all properties of the given queue.</returns>
    function GetQueueProperties(const QueueURL: string; Attribute: TAmazonQueueAttribute = aqaAll;
                                ResponseInfo: TCloudResponseInfo = nil): TStrings; overload;
    /// <summary>Returns one or more properties for the specified Queue.</summary>
    /// <remarks>The attributes specified will be the property returned by this call. If you specify 'All'
    ///          as an attribute to return, then all properties of the queue will be returned.
    ///          If you pass an empty array, then all attributes will be returned.
    /// </remarks>
    // / <param name="QueueURL">The URL of the Queue to get the attributes for</param>
    // / <param name="Attributes">The attributes (or All) to get</param>
    // / <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>Returns one or more properties of the given queue.</returns>
    function GetQueueProperties(const QueueURL: string; Attributes: array of TAmazonQueueAttribute;
                                ResponseInfo: TCloudResponseInfo = nil): TStrings; overload;
    /// <summary>Sets the given queue's attributes</summary>
    /// <remarks>The supported attributes that can be set are:
    ///          VisibilityTimeout - The time (in seconds) that a received message will be hidden from other
    ///          Policy - The formal description of the permissions for a resource (JSON object as string)
    ///          MaximumMessageSize - how many bytes a message can contain before Amazon SQS rejects it
    ///          MessageRetentionPeriod - The number of seconds Amazon SQS retains a message.
    ///
    ///          For more information on the format of the Policy value, view Amazon's Queue Service documentation.
    /// </remarks>
    /// <param name="QueueURL">The URL of the Queue to set the attributes for</param>
    /// <param name="Key">The name of the attribute to set. See remarks for more information.</param>
    /// <param name="Value">The value of the attribute to set. See remarks for more information.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the operation was successful, false otherwise.</returns>
    function SetQueueProperty(const QueueURL, Key, Value: string;
                              ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Adds the given permissions to the specified queue.</summary>
    /// <remarks>The specified label will uniquely identify the permission being set.
    ///          The label must be a maximum of 80 characters;
    ///            alphanumeric characters, hyphens (-), and underscores (_) are allowed.
    /// </remarks>
    // / <param name="QueueURL">The URL of the Queue to add the permissions to.</param>
    // / <param name="PermissionsLabel">The unique identifier for these permissions.</param>
    // / <param name="Permissions">The permissions to add.</param>
    // / <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the operation was successful, false otherwise.</returns>
    function AddQueuePermissions(const QueueURL, PermissionsLabel: string;
                                 Permissions: array of TAmazonQueuePermission;
                                 ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Removes the permissions with the given label from the specified queue.</summary>
    /// <param name="QueueURL">The URL of the Queue to remove the permissions from.</param>
    /// <param name="PermissionsLabel">The unique identifier for the permissions to remove.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the operation was successful, false otherwise.</returns>
    function RemoveQueuePermissions(const QueueURL, PermissionsLabel: string;
                                    ResponseInfo: TCloudResponseInfo = nil): Boolean;
    /// <summary>Deletes the messages in a queue specified by the queue URL.</summary>
    /// <param name="AQueueURL">The URL of the Queue to remove the messages from.</param>
    /// <param name="AResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the operation was successful, false otherwise.</returns>
    function PurgeQueue(const AQueueURL: string; const AResponseInfo: TCloudResponseInfo): Boolean;
    /// <summary>Adds a message to the given queue</summary>
    /// <param name="QueueURL">The URL of the queue to add the message to</param>
    /// <param name="MessageText">The text of the message</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the message was added successfully, false otherwise.</returns>
    function AddMessage(const QueueURL: string; const MessageText: string;
                        ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Adds a message to the given queue</summary>
    /// <param name="QueueURL">The URL of the queue to add the message to</param>
    /// <param name="MessageText">The text of the message</param>
    /// <param name="MessageId">The Id of the message in the queue, or empty string if add failed.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the message was added successfully, false otherwise.</returns>
    function AddMessage(const QueueURL: string; const MessageText: string; out MessageId: string;
                        ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Returns messages from the given queue.</summary>
    /// <remarks>If NumOfMessages isn't set, then the server default of 1 is used. The maximum allowed value is 10.
    ///          If VisibilityTimeout isn't set, the queue's default is used. The maximum allowed
    ///          value is 12 hours.
    ///          Note that messages returned by this call will have their PopReceipt specified, which is a
    ///          token unique to the message during the VisibilityTimeout which can be used to delete the message.
    /// </remarks>
    /// <param name="QueueURL">The URL of the queue to get the messages for</param>
    /// <param name="NumOfMessages">The maximum number of messages to return.</param>
    /// <param name="VisibilityTimeout">How long the messages should be reserved for</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The XML representation of the messages</returns>
    function GetMessagesXML(const QueueURL: string;
                            NumOfMessages: Integer = 0;
                            VisibilityTimeout: Integer = -1;
                            ResponseInfo: TCloudResponseInfo = nil): string;
    /// <summary>Returns messages from the given queue.</summary>
    /// <remarks>If NumOfMessages isn't set, then the server default of 1 is used. The maximum allowed value is 10.
    ///          If VisibilityTimeout isn't set, the queue's default is used. The maximum allowed
    ///          value is 12 hours.
    ///          Note that messages returned by this call will have their PopReceipt specified, which is a
    ///          token unique to the message during the VisibilityTimeout which can be used to delete the message.
    /// </remarks>
    /// <param name="QueueURL">The URL of the queue to get the messages for</param>
    /// <param name="NumOfMessages">The maximum number of messages to return.</param>
    /// <param name="VisibilityTimeout">How long the messages should be reserved for</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The list of messages, with their pop receipts populated</returns>
    function GetMessages(const QueueURL: string;
                         NumOfMessages: Integer = 0;
                         VisibilityTimeout: Integer = -1;
                         ResponseInfo: TCloudResponseInfo = nil): TList<TCloudQueueMessage>;
    /// <summary>Returns messages from the given queue.</summary>
    /// <remarks>If NumOfMessages isn't set, then the server default of 1 is used. The maximum allowed value is 10.
    ///          Note that messages returned by this call will NOT have their PopReceipt specified. There is
    ///          no VisibilityTimeout, and so other people can instantly query messages and see them.
    /// </remarks>
    /// <param name="QueueURL">The URL of the queue to get the messages for</param>
    /// <param name="NumOfMessages">The maximum number of messages to return.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>The list of messages</returns>
    function PeekMessages(const QueueURL: string; NumOfMessages: Integer;
                          ResponseInfo: TCloudResponseInfo = nil): TList<TCloudQueueMessage>;
    /// <summary>Deletes the given message from the specified queue</summary>
    /// <param name="QueueURL">URL of the queue to delete a message from</param>
    /// <param name="PopReceipt">The pop receipt required for deleting the message</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the delete was successful, false otherwise</returns>
    function DeleteMessage(const QueueURL: string; const PopReceipt: string;
                           ResponseInfo: TCloudResponseInfo = nil): Boolean; overload;
    /// <summary>Extends or ends the visibility timeout of a message.</summary>
    /// <remarks>If zero is passed in as the VisibilityTimeout, then the message instantly becomes
    ///          visible by calls to GetMessages. Otherwise, the value passed in, which must be
    ///          between o and 43200 seconds (12 hours) and will be set as the new value for
    ///          VisibilityTimeout for the message associated with the given pop receipt, if any.
    /// </remarks>
    /// <param name="QueueURL">The URL of the queue to get the messages for</param>
    /// <param name="PopReceipt">The previously obtained pop receipt. Associated with a message.</param>
    /// <param name="VisibilityTimeout">Time (in seconds) to have the message hidden for.</param>
    /// <param name="ResponseInfo">The optional class for storing response info into</param>
    /// <returns>True if the operation is successful, false otherwise.</returns>
    function ChangeMessageVisibility(const QueueURL: string; const PopReceipt: string;
                                     const VisibilityTimeout: Integer = 0;
                                     ResponseInfo: TCloudResponseInfo = nil): Boolean;
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

{ TAmazonChinaQueueService }


function TAmazonChinaQueueService.GetQueueAttributeName(const Attribute: TAmazonQueueAttribute): string;
begin
  case Attribute of
    aqaAll:                                   Exit('All');
    aqaApproximateNumberOfMessages:           Exit('ApproximateNumberOfMessages');
    aqaApproximateNumberOfMessagesNotVisible: Exit('ApproximateNumberOfMessagesNotVisible');
    aqaVisibilityTimeout:                     Exit('VisibilityTimeout');
    aqaCreatedTimestamp:                      Exit('CreatedTimestamp');
    aqaLastModifiedTimestamp:                 Exit('LastModifiedTimestamp');
    aqaPolicy:                                Exit('Policy');
    aqaMaximumMessageSize:                    Exit('MaximumMessageSize');
    aqaMessageRetentionPeriod:                Exit('MessageRetentionPeriod');
    aqaQueueArn:                              Exit('QueueArn');
    aqaApproximateNumberOfMessagesDelayed:    Exit('ApproximateNumberOfMessagesDelayed');
    aqaDelaySeconds:                          Exit('DelaySeconds');
    aqaReceiveMessageWaitTimeSeconds:         Exit('ReceiveMessageWaitTimeSeconds');
    aqaRedrivePolicy:                         Exit('RedrivePolicy');
  end;
end;

function TAmazonChinaQueueService.ListQueuesXML(OptionalParams: TStrings;
                                           ResponseInfo: TCloudResponseInfo): string;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'ListQueues');

  if OptionalParams <> nil then
    QueryParams.AddStrings(OptionalParams);

  Response := nil;
  try
    Response := IssueRequest(GetConnectionInfo.QueueURL, QueryParams, ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaQueueService.ListQueues(OptionalParams: TStrings; ResponseInfo: TCloudResponseInfo): TList<TCloudQueue>;
var
  xml: string;
  xmlDoc: IXMLDocument;
  QueuesXMLNode: IXMLNode;
  QueueNode: IXMLNode;
begin
  xml := ListQueuesXML(OptionalParams, ResponseInfo);

  if XML = EmptyStr then
    Exit(nil);

  Result := TList<TCloudQueue>.Create;

  xmlDoc := TXMLDocument.Create(nil);
  xmlDoc.LoadFromXML(XML);

  QueuesXMLNode := xmlDoc.DocumentElement.ChildNodes.FindNode(NODE_QUEUES);

  if (QueuesXMLNode <> nil) and (QueuesXMLNode.HasChildNodes) then
  begin
    QueueNode := QueuesXMLNode.ChildNodes.FindNode(NODE_QUEUE);

    while (QueueNode <> nil) do
    begin
      if QueueNode.NodeName = NODE_QUEUE then
      begin
        //the Queue node ('QueueUrl') has only a text value, which is the Queue URL
        Result.Add(TCloudQueue.Create(QueueNode.Text));
      end;
      QueueNode := QueueNode.NextSibling;
    end;
  end;
end;

function TAmazonChinaQueueService.CreateQueue(const QueueName: string; const DefaultVisibilityTimeout: Integer;
                                         ResponseInfo: TCloudResponseInfo): Boolean;
var
  URL: string;
begin
  Result := CreateQueue(QueueName, URL, DefaultVisibilityTimeout, ResponseInfo);
end;

function TAmazonChinaQueueService.CreateQueue(const QueueName: string; out QueueURL: string;
                                         const DefaultVisibilityTimeout: Integer;
                                         ResponseInfo: TCloudResponseInfo): Boolean;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
  xml: string;
  xmlDoc: IXMLDocument;
  ResultNode, QueueNode: IXMLNode;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'CreateQueue');

  QueryParams.Values['QueueName'] := QueueName;

  if DefaultVisibilityTimeout > -1 then
  begin
    QueryParams.Values['Attribute.1.Name'] := 'VisibilityTimeout';
    QueryParams.Values['Attribute.1.Value'] := IntToStr(DefaultVisibilityTimeout);
  end;

  Response := nil;
  try
    Response := IssueRequest(GetConnectionInfo.QueueURL, QueryParams, ResponseInfo, xml);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
    FreeAndNil(Response);

    if Result and (xml <> EmptyStr) then
    begin
      //Parse XML and get QueueURL value
      xmlDoc := TXMLDocument.Create(nil);
      xmlDoc.LoadFromXML(XML);

      ResultNode := xmlDoc.DocumentElement.ChildNodes.FindNode(NODE_QUEUE_CREATE_RESULT);

      if (ResultNode <> nil) and (ResultNode.HasChildNodes) then
      begin
        QueueNode := ResultNode.ChildNodes.FindNode(NODE_QUEUE);
        if (QueueNode <> nil) and (QueueNode.IsTextElement) then
          QueueURL := QueueNode.Text;
      end;
    end;
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaQueueService.CreateQueue(const QueueName: string; out QueueURL: string;
                                         Attributes: TArray<TPair<TAmazonQueueAttribute, string>>;
                                         ResponseInfo: TCloudResponseInfo = nil): Boolean;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
  attribute: TPair<TAmazonQueueAttribute, string>;
  nattr: Integer;
  xml: string;
  xmlDoc: IXMLDocument;
  ResultNode, QueueNode: IXMLNode;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'CreateQueue');

  QueryParams.Values['QueueName'] := QueueName;

  for nAttr := 0 to Length(Attributes)-1 do
  begin
    attribute := Attributes[nAttr];

    QueryParams.Values[Format('Attribute.%d.Name',[nAttr+1])] := GetQueueAttributeName(attribute.key);
    QueryParams.Values[Format('Attribute.%d.Value',[nAttr+1])] := attribute.Value;
  end;

  Response := nil;
  try
    Response := IssueRequest(GetConnectionInfo.QueueURL, QueryParams, ResponseInfo, xml);
    Result := (Response <> nil) and (Response.ResponseCode = 200);

    if Result and (xml <> EmptyStr) then
    begin
      //Parse XML and get QueueURL value
      xmlDoc := TXMLDocument.Create(nil);
      xmlDoc.LoadFromXML(XML);

      ResultNode := xmlDoc.DocumentElement.ChildNodes.FindNode(NODE_QUEUE_CREATE_RESULT);

      if (ResultNode <> nil) and (ResultNode.HasChildNodes) then
      begin
        QueueNode := ResultNode.ChildNodes.FindNode(NODE_QUEUE);
        if (QueueNode <> nil) and (QueueNode.IsTextElement) then
          QueueURL := QueueNode.Text;
      end;
    end;
  finally
    Response.Free;
    QueryParams.Free;
  end;
end;

function TAmazonChinaQueueService.DeleteMessage(const QueueURL, PopReceipt: string;
                                           ResponseInfo: TCloudResponseInfo): Boolean;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'DeleteMessage');

  QueryParams.Values['ReceiptHandle'] := PopReceipt;

  Response := nil;
  try
    Response := IssueRequest(QueueURL, QueryParams, ResponseInfo);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaQueueService.DeleteQueue(const QueueURL: string; ResponseInfo: TCloudResponseInfo): Boolean;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'DeleteQueue');

  Response := nil;
  try
    Response := IssueRequest(QueueURL, QueryParams, ResponseInfo);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaQueueService.GetQueuePropertiesXML(const QueueURL: string;
                                                   Attributes: array of TAmazonQueueAttribute;
                                                   ResponseInfo: TCloudResponseInfo): string;
const
  AttribParamName = 'AttributeName';
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
  Length, I, Index: Integer;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'GetQueueAttributes');

  Length := (High(Attributes) + 1) - Low(Attributes);

  if Length <= 0 then
    QueryParams.Values[AttribParamName] := 'All'
  else if Length = 1 then
     QueryParams.Values[AttribParamName] := GetQueueAttributeName(Attributes[Low(Attributes)])
  else
  begin
    Index := 1;
    for I := Low(Attributes) to High(Attributes) do
    begin
       QueryParams.Values[Format('%s.%d', [AttribParamName, Index])] := GetQueueAttributeName(Attributes[I]);
       Inc(Index);
    end;
  end;

  Response := nil;
  try
    Response := IssueRequest(QueueURL, QueryParams, ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaQueueService.GetServiceHost: string;
begin
  Result := GetConnectionInfo.QueueEndpoint;
end;

function TAmazonChinaQueueService.GetServiceVersion: string;
begin
  Result := '2012-11-05';
end;

function TAmazonChinaQueueService.GetQueuePropertiesXML(const QueueURL: string; Attribute: TAmazonQueueAttribute;
                                                   ResponseInfo: TCloudResponseInfo): string;
begin
  Result := GetQueuePropertiesXML(QueueURL, [Attribute], ResponseInfo);
end;

function TAmazonChinaQueueService.GetQueueProperties(const QueueURL: string; Attribute: TAmazonQueueAttribute;
                                                ResponseInfo: TCloudResponseInfo): TStrings;
begin
  Result := GetQueueProperties(QueueURL, [Attribute], ResponseInfo);
end;

function TAmazonChinaQueueService.GetQueueProperties(const QueueURL: string;
                                                Attributes: array of TAmazonQueueAttribute;
                                                ResponseInfo: TCloudResponseInfo): TStrings;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ResultNode, AttribNode, Aux: IXMLNode;
  Name: string;
begin
  Result := nil;
  xml := GetQueuePropertiesXML(QueueURL, Attributes, ResponseInfo);

  if xml <> emptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);
    xmlDoc.LoadFromXML(xml);

    RootNode := xmlDoc.DocumentElement;

    if RootNode.HasChildNodes then
    begin
      ResultNode := RootNode.ChildNodes.FindNode(NODE_QUEUE_ATTRIBS_RESULT);
      if (ResultNode <> nil) then
        Result := TStringList.Create;

      if (ResultNode <> nil) and ResultNode.HasChildNodes then
      begin
        AttribNode := GetFirstMatchingChildNode(ResultNode, NODE_ATTRIBUTES);

        while AttribNode <> nil do
        begin
          try
            Aux := AttribNode.ChildNodes.FindNode(NODE_NAME);
            if (Aux <> nil) and Aux.IsTextElement then
            begin
              Name := Aux.Text;
              Aux := AttribNode.ChildNodes.FindNode(NODE_VALUE);

              if (Aux <> nil) and Aux.IsTextElement then
                Result.Values[Name] := Aux.Text;
            end;
          finally
            AttribNode := AttribNode.NextSibling;
          end;
        end;
      end;
    end;
  end;
end;

function TAmazonChinaQueueService.SetQueueProperty(const QueueURL, Key, Value: string;
                                                ResponseInfo: TCloudResponseInfo): Boolean;
const
  AttribParam = 'Attribute';
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
begin
  Result := False;

  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'SetQueueAttributes');

  if Key <> EmptyStr then
  begin
    QueryParams.Values['Attribute.Name'] := Key;
    QueryParams.Values['Attribute.Value'] := Value;

    Response := nil;
    try
      Response := IssueRequest(QueueURL, QueryParams, ResponseInfo);
      Result := (Response <> nil) and (Response.ResponseCode = 200);
    finally
      if Assigned(Response) then
        FreeAndNil(Response);
      FreeAndNil(QueryParams);
    end;
  end;
end;

function TAmazonChinaQueueService.AddQueuePermissions(const QueueURL, PermissionsLabel: string;
                                                 Permissions: array of TAmazonQueuePermission;
                                                 ResponseInfo: TCloudResponseInfo): Boolean;
const
  AttribParam = 'AWSAccountId';
  AttributeVal = 'ActionName';
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
  Length, Index, I: Integer;
  Perm: TAmazonQueuePermission;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'AddPermission');

  QueryParams.Values['Label'] := PermissionsLabel;

  Length := (High(Permissions) + 1) - Low(Permissions);

  if Length > 0 then
  begin
    Index := 1;
    for I := Low(Permissions) to High(Permissions) do
    begin
      Perm := Permissions[I];

      QueryParams.Values[Format('%s.%d', [AttribParam, Index])] := Perm.AccountId;
      QueryParams.Values[Format('%s.%d', [AttributeVal, Index])] := Perm.GetAction;
      Inc(Index);
    end;
  end;

  Response := nil;
  try
    Response := IssueRequest(QueueURL, QueryParams, ResponseInfo);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaQueueService.RemoveQueuePermissions(const QueueURL, PermissionsLabel: string;
                                                    ResponseInfo: TCloudResponseInfo): Boolean;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'RemovePermission');

  QueryParams.Values['Label'] := PermissionsLabel;

  Response := nil;
  try
    Response := IssueRequest(QueueURL, QueryParams, ResponseInfo);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaQueueService.PurgeQueue(const AQueueURL: string; const AResponseInfo: TCloudResponseInfo): Boolean;
var
  LQueryParams: TStringList;
  LResponse: TCloudHTTP;
begin
  LResponse := nil;
  LQueryParams := nil;
  try
    LQueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'PurgeQueue');
    LResponse := IssueRequest(AQueueURL, LQueryParams, AResponseInfo);
    Result := (LResponse <> nil) and (LResponse.ResponseCode = 200);
  finally
    LQueryParams.Free;
    LResponse.Free;
  end;
end;

function TAmazonChinaQueueService.AddMessage(const QueueURL, MessageText: string;
                                        ResponseInfo: TCloudResponseInfo): Boolean;
var
  MsgId: string;
begin
  Result := AddMessage(QueueURL, MessageText, MsgId, ResponseInfo);
end;

function TAmazonChinaQueueService.AddMessage(const QueueURL, MessageText: string; out MessageId: string;
                                        ResponseInfo: TCloudResponseInfo): Boolean;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ResultNode, IdNode: IXMLNode;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'SendMessage');

  QueryParams.Values['MessageBody'] := MessageText;

  Response := nil;
  try
    Response := IssueRequest(QueueURL, QueryParams, ResponseInfo, xml);
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

function TAmazonChinaQueueService.GetMessagesXML(const QueueURL: string; NumOfMessages, VisibilityTimeout: Integer;
                                            ResponseInfo: TCloudResponseInfo): string;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'ReceiveMessage');

  //get all attributes associated with the message
  QueryParams.Values['AttributeName'] := 'All';

  if NumOfMessages > 0 then
    QueryParams.Values['MaxNumberOfMessages'] := IntToStr(NumOfMessages);

  if VisibilityTimeout > -1 then
    QueryParams.Values['VisibilityTimeout'] := IntToStr(VisibilityTimeout);

  Response := nil;
  try
    Response := IssueRequest(QueueURL, QueryParams, ResponseInfo, Result);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

function TAmazonChinaQueueService.GetMaxMessageReturnCount: Integer;
begin
  Result := 10;
end;

function TAmazonChinaQueueService.IsUniqueMessageId(const MessageId: string;
                          const MessageList: TList<TCloudQueueMessage>): Boolean;
var
  Item: TCloudQueueMessage;
begin
  Result := False;
  if (MessageId <> EmptyStr) and (MessageList <> nil) then
  begin
    if MessageList.Count = 0 then
      Exit(True);

    //MessageId is not valid if the given list already contains it
    for Item in MessageList do
      if Item.MessageId = MessageId then
        Exit(False);

    Result := True;
  end;
end;

function TAmazonChinaQueueService.GetMessages(const QueueURL: string; NumOfMessages, VisibilityTimeout: Integer;
                                         ResponseInfo: TCloudResponseInfo): TList<TCloudQueueMessage>;
var
  xml: string;
  xmlDoc: IXMLDocument;
  RootNode, ResultNode, MessageNode, ItemNode: IXMLNode;
  MessageId: string;
  Item: TCloudQueueMessage;
begin
  Result := nil;

  xml := GetMessagesXML(QueueURL, NumOfMessages, VisibilityTimeout, ResponseInfo);

  if xml <> EmptyStr then
  begin
    xmlDoc := TXMLDocument.Create(nil);
    xmlDoc.LoadFromXML(xml);

    RootNode := xmlDoc.DocumentElement;

    if RootNode.HasChildNodes then
    begin
      ResultNode := RootNode.ChildNodes.FindNode(NODE_QUEUE_MESSAGE_RECEIVE_RESULT);
      if (ResultNode <> nil) then
        Result := TList<TCloudQueueMessage>.Create;

      if (ResultNode <> nil) and ResultNode.HasChildNodes then
      begin
        MessageNode := GetFirstMatchingChildNode(ResultNode, NODE_QUEUE_MESSAGE);

        while MessageNode <> nil do
        begin
          Item := nil;
          try
            //Get the MessageId, then if that is siccessful, populate the message body
            //all other attributes are optional
            ItemNode := MessageNode.ChildNodes.FindNode(NODE_QUEUE_MESSAGE_ID);
            if (ItemNode <> nil) and ItemNode.IsTextElement then
            begin
              MessageId := ItemNode.Text;
              if IsUniqueMessageId(MessageId, Result) then
              begin
                ItemNode := MessageNode.ChildNodes.FindNode(NODE_QUEUE_MESSAGE_BODY);

                if (ItemNode <> nil) and ItemNode.IsTextElement then
                  Item := TCloudQueueMessage.Create(MessageId, ItemNode.Text);
              end;
            end;

            //populate optional attributes and pop receipt
            if Item <> nil then
            begin
              //populate the pop receipt (called ReceiptHandle in Amazon)
              //only do this if VisibilityTimeout was set to something greater than zero.
              if VisibilityTimeout > 0 then
              begin
                ItemNode := MessageNode.ChildNodes.FindNode(NODE_QUEUE_MESSAGE_POPRECEIPT);
                if (ItemNode <> nil) and ItemNode.IsTextElement then
                  Item.PopReceipt := ItemNode.Text;
              end;

              ItemNode := MessageNode.ChildNodes.FindNode(NODE_QUEUE_MESSAGE_MD5);
              if (ItemNode <> nil) and ItemNode.IsTextElement then
                Item.Properties.Values[NODE_QUEUE_MESSAGE_MD5] := ItemNode.Text;

              //populate the other attributes
              PopulateKeyValuePairs(MessageNode, Item.Properties, NODE_ATTRIBUTES);
            end;
          finally
            MessageNode := MessageNode.NextSibling;
            if Item <> nil then
              Result.Add(Item);
          end;
        end;
      end;
    end;
  end;
end;

function TAmazonChinaQueueService.PeekMessages(const QueueURL: string; NumOfMessages: Integer;
                                          ResponseInfo: TCloudResponseInfo): TList<TCloudQueueMessage>;
begin
  //Set VisibilityTimeout to 0, so that messages are instantly visible to other callers.
  Result := GetMessages(QueueURL, NumOfMessages, 0, ResponseInfo);
end;

function TAmazonChinaQueueService.ChangeMessageVisibility(const QueueURL, PopReceipt: string;
  const VisibilityTimeout: Integer; ResponseInfo: TCloudResponseInfo): Boolean;
var
  Response: TCloudHTTP;
  QueryParams: TStringList;
begin
  QueryParams := BuildQueryParameters(TAmazonChinaServiceType.csSQS, 'ChangeMessageVisibility');

  QueryParams.Values['ReceiptHandle'] := PopReceipt;
  QueryParams.Values['VisibilityTimeout'] := IntToStr(VisibilityTimeout);

  Response := nil;
  try
    Response := IssueRequest(QueueURL, QueryParams, ResponseInfo);
    Result := (Response <> nil) and (Response.ResponseCode = 200);
  finally
    if Assigned(Response) then
      FreeAndNil(Response);
    FreeAndNil(QueryParams);
  end;
end;

procedure TAmazonChinaQueueService.PrepareRequestSignature(const HTTPVerb: string;
                                  const Headers, QueryParameters: TStringList;
                                  const StringToSign: string;
                                  var URL: string; Request: TCloudHTTP; var Content: TStream);
begin
  PrepareRequestQuerySignatureByService(TAmazonChinaServiceType.csSQS, HTTPVerb, Headers,
    QueryParameters, StringToSign, URL, Request, Content);

end;

function TAmazonChinaQueueService.BuildStringToSign(const HTTPVerb: string; Headers, QueryParameters: TStringList;
                          const QueryPrefix, URL: string): string;
var
  LdateISO, Ldate, Lregion, Scope:string;
begin
  Lregion := GetRegionFromEndpoint(TAmazonChinaServiceType.csSQS, URL);

  Result:= inherited BuildStringToSign(HTTPVerb, Headers, QueryParameters, QueryPrefix, URL);
  //Host
  Result := Result + #10'host:'+GetServiceHost+#10;
  Result := Result + #10'host';
  Result := Result + #10'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';

  Result := TAmazonChinaAWS4Authentication(FAuthenticator).BuildQueryAuthorizationString(Result, Lregion, 'sqs', QueryParameters);

end;

end.
