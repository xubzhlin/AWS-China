unit uAWSRestServiceUnit;

interface

uses
  System.SysUtils, uDataDefineUnit,
  Data.Cloud.AmazonAPI.China, Data.Cloud.AmazonAPI.China.GateWay, Data.Cloud.AmazonAPI.China.S3;


type
  TAWSRestService = class(TObject)
  private
    FConnectionInfo:TAmazonChinaConnectionInfo;
    FGateWayService:TAmazonChinaGateWayService;
    FS3Service:TAmazonChinaStorageService;
  public
    constructor Create(const AccountName, AccountPass: string);
    destructor Destroy; override;

    property GateWay:TAmazonChinaGateWayService read FGateWayService;
    property S3:TAmazonChinaStorageService read FS3Service;
  end;

var
  GAWSRestService:TAWSRestService;

implementation

{ TAWSGateWayService }

constructor TAWSRestService.Create(const AccountName, AccountPass: string);
begin
  inherited Create;

  FConnectionInfo := TAmazonChinaConnectionInfo.Create(nil);
  FConnectionInfo.AccountName := AccountName;
  FConnectionInfo.AccountKey := AccountPass;
  FConnectionInfo.Protocol:='https';
  FConnectionInfo.GateWayEndPoint:='execute-api.cn-north-1.amazonaws.com.cn';
  FConnectionInfo.StorageEndpoint:='s3.cn-north-1.amazonaws.com.cn';
  FGateWayService:= TAmazonChinaGateWayService.Create(FConnectionInfo);
  FS3Service:= TAmazonChinaStorageService.Create(FConnectionInfo);
end;


destructor TAWSRestService.Destroy;
begin
  FGateWayService.Free;
  FS3Service.Free;
  FConnectionInfo.Free;
  inherited;
end;

end.
