unit uAWSIotServiceUnit;

interface

uses
  System.SysUtils, uDataDefineUnit, uAWSChatCommUnit,
  AWS.Core, AWS.IoT, AWS.Iot.Comm;

type
  TAWSIotPubSubService = class(TObject)
  private
    FOnAWSIotMqttClientStatus:TOnAWSIotMqttClientStatus;
    FOnAWSIoTMQTTNewMessage:TOnAWSIoTMQTTNewMessage;
    FServiceConfiguration:TAWSIoTServiceConfiguration;
    FIoTService:TAWSIoTService;
    procedure DoAWSIotMqttClientStatus(Status:TAWSIotMqttClientStatus);
    procedure DoAWSIoTMQTTNewMessage(NewMessage:string);
    function CreateAWSIoTTopicConfiguration(AWSIotTopic:TAWSIotTopicRec):TAWSIoTTopicConfiguration;
    function GetIotService: TAWSIotService;

    property IotService:TAWSIotService read GetIotService;
  public
    constructor Create;
    destructor Destroy; override;

    procedure Connecte;
    procedure DisConnecte;

    procedure PublishString(AWSIotTopic:TAWSIotTopicRec; AWSIotMsg:TAWSIotMsgRec);
    procedure SubscribeToTopic(AWSIotTopic:TAWSIotTopicRec);
    procedure UnsubscribeTopic(AWSIotTopic:TAWSIotTopicRec); overload;
    procedure UnsubscribeTopic; overload;

    property OnAWSIotMqttClientStatus:TOnAWSIotMqttClientStatus read FOnAWSIotMqttClientStatus write FOnAWSIotMqttClientStatus;
    property OnAWSIoTMQTTNewMessage:TOnAWSIoTMQTTNewMessage read FOnAWSIoTMQTTNewMessage write FOnAWSIoTMQTTNewMessage;
  end;

var
  GAWSIotPubSubService:TAWSIotPubSubService;

implementation

uses System.JSON, System.IOUtils;

{ TAWSIotPubSubService }

procedure TAWSIotPubSubService.Connecte;
begin
  IoTService.Connected:=True;
end;

constructor TAWSIotPubSubService.Create;
begin
  inherited Create;

  FServiceConfiguration.CognitoIdentityPoolId:='MyCognitoIdentityPoolId';

//AWS Iot –¬µƒ≈‰÷√
{$IFDEF Android}
  FServiceConfiguration.IoT_EndPoint:='a2xiglk2v2f2wn.iot.cn-north-1.amazonaws.com.cn';
  FServiceConfiguration.KeystoreFilePath:=TPath.GetDocumentsPath;
  FServiceConfiguration.KeystoreName:='awsiotidentity.bks';
  FServiceConfiguration.KeystorePassword:='hx123456!';
  FServiceConfiguration.CertificateId:='awsiot';
{$ENDIF}
{$IFDEF iOS}
  FServiceConfiguration.IoT_EndPoint:='https://a2xiglk2v2f2wn.iot.cn-north-1.amazonaws.com.cn';
  FServiceConfiguration.P12FilePath:=TPath.GetDocumentsPath + TPath.DirectorySeparatorChar + 'awsiotidentity.p12';
  FServiceConfiguration.P12PassPhrase:='hx123456!';
{$ENDIF}
  FServiceConfiguration.Region := TAWSAWSRegionType.AWSRegionCNNorthWest1;
  FServiceConfiguration.DataManagerKey:='iotDataManagerKey';

//AWS Iot æ…µƒ≈‰÷√
//{$IFDEF Android}
//  FServiceConfiguration.IoT_EndPoint:='a2cz9eza4v6bkw.iot.cn-north-1.amazonaws.com.cn';
//  FServiceConfiguration.KeystoreFilePath:=TPath.GetDocumentsPath;
//  FServiceConfiguration.KeystoreName:='awsiot.bks';
//  FServiceConfiguration.KeystorePassword:='hx123456';
//  FServiceConfiguration.CertificateId:='awsiot';
//{$ENDIF}
//{$IFDEF iOS}
//  FServiceConfiguration.IoT_EndPoint:='https://a2cz9eza4v6bkw.iot.cn-north-1.amazonaws.com.cn';
//  FServiceConfiguration.P12FilePath:=TPath.GetDocumentsPath + TPath.DirectorySeparatorChar + 'awsiot.p12';
//  FServiceConfiguration.P12PassPhrase:='hx123456';
//{$ENDIF}
//  FServiceConfiguration.Region := TAWSAWSRegionType.AWSRegionCNNorthWest1;
//  FServiceConfiguration.DataManagerKey:='iotDataManagerKey';

end;

function TAWSIotPubSubService.CreateAWSIoTTopicConfiguration(
  AWSIotTopic: TAWSIotTopicRec):TAWSIoTTopicConfiguration;
begin
  //FOSSMNT/HLH/
  case AWSIotTopic._ChatClassType of
    cgtNone:Result.Topic:='';
    cgtPrivate:Result.Topic:=AWSIotTopic._Topic;
    cgtFlight:Result.Topic:=AWSIotTopic._Topic;
    cgtDepartment:Result.Topic:=AWSIotTopic._Topic;
    cgtGroup:Result.Topic:=AWSIotTopic._Topic;
    cgtAll:Result.Topic:='FOSSMNT/#';
  end;
  Result.Qos:=AWSIotTopic._Qos;
end;

procedure TAWSIotPubSubService.DoAWSIotMqttClientStatus(
  Status: TAWSIotMqttClientStatus);
begin
  if Assigned(FOnAWSIotMqttClientStatus) then
    FOnAWSIotMqttClientStatus(Status);
end;

destructor TAWSIotPubSubService.Destroy;
begin
  AWSIoTServiceManager.DestoryService(FIoTService);
  inherited;
end;

procedure TAWSIotPubSubService.DisConnecte;
begin
  if FIoTService<>nil then
    FIoTService.Connected:=False;
end;

procedure TAWSIotPubSubService.DoAWSIoTMQTTNewMessage(NewMessage: string);
begin
  if Assigned(FOnAWSIoTMQTTNewMessage) then
    FOnAWSIoTMQTTNewMessage(NewMessage);
end;

function TAWSIotPubSubService.GetIotService: TAWSIotService;
begin
  if FIoTService = nil then
  begin
    FIoTService:=AWSIoTServiceManager.CreateServiceByConfiguration(FServiceConfiguration);
    FIoTService.OnAWSIotMqttClientStatus:=DoAWSIotMqttClientStatus;
  end;
  Result:=FIoTService;
end;

procedure TAWSIotPubSubService.PublishString(AWSIotTopic:TAWSIotTopicRec; AWSIotMsg:TAWSIotMsgRec);
var
  TopConfiguration:TAWSIoTTopicConfiguration;
  Msg:TJSONObject;
begin
  if FIoTService<>nil then
  begin
    TopConfiguration:=CreateAWSIoTTopicConfiguration(AWSIotTopic);
    if TopConfiguration.Topic = '' then  exit;

    Msg:=TJSONObject.Create;
    Msg.AddPair('ChatClassType', TJSONNumber.Create(Ord(AWSIotTopic._ChatClassType)));
    Msg.AddPair('ReceiverID', TJSONNumber.Create(AWSIotTopic._ReceiverID));
    Msg.AddPair('EmployeeNum', TJSONNumber.Create(AWSIotMsg._EmployeeNum));
    Msg.AddPair('Chatstring', AWSIotMsg._Chatstring);
    FIoTService.PublishString(TopConfiguration, Msg.ToJSON);
  end;
end;

procedure TAWSIotPubSubService.SubscribeToTopic(AWSIotTopic:TAWSIotTopicRec);
var
  TopConfiguration:TAWSIoTTopicConfiguration;
  Topic:TAWSIotTopic;
begin
  if FIoTService<>nil then
  begin
    TopConfiguration:=CreateAWSIoTTopicConfiguration(AWSIotTopic);
    if TopConfiguration.Topic = '' then  exit;
    Topic:=FIoTService.SubscribeToTopicQoSmessage(TopConfiguration);
    if Topic<>nil then
      Topic.OnAWSIoTMQTTNewMessage:=DoAWSIoTMQTTNewMessage;
  end;
end;

procedure TAWSIotPubSubService.UnsubscribeTopic;
begin
  if FIoTService<>nil then
  begin
    FIoTService.UnsubscribeTopic;
  end;
end;

procedure TAWSIotPubSubService.UnsubscribeTopic(AWSIotTopic:TAWSIotTopicRec);
var
  TopConfiguration:TAWSIoTTopicConfiguration;
begin
  if FIoTService<>nil then
  begin
    TopConfiguration:=CreateAWSIoTTopicConfiguration(AWSIotTopic);
    if TopConfiguration.Topic = '' then  exit;
    FIoTService.UnsubscribeTopic(TopConfiguration.Topic);
  end;

end;

end.
