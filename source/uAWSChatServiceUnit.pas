unit uAWSChatServiceUnit;


interface

uses
  System.Classes, System.SysUtils, FMX.Graphics;

type
  TAWSChatService = class(TObject)
  public
    class function S3UpObject(const FilePath:string;  const SavePath:string = ''; const ObjectPath:string = ''; const Ext:string = ''):string; overload;
    class function S3UpObject(const Stream:TStream;  const SavePath:string = ''; const ObjectPath:string = ''; const Ext:string = ''):string; overload;
    class function S3UpObject(const Buffer: TBytes; ObjectName:string; const SavePath:string = ''; const ObjectPath:string = ''; const Ext:string = ''):Boolean; overload;

    class function S3UpImageObject(const BitMap:TBitMap; const SaveParams: PBitmapCodecSaveParams = nil; const SavePath:string = ''; const Ext:string = ''):string;
    class function S3UpAudoiObject(const Stream:TFileStream; const SavePath:string = ''; const Ext:string = ''):string;
    class function S3UpVideoObject(const Stream:TFileStream; const SavePath:string = ''; const Ext:string = ''):string;

    class function S3GetObject(const FilePath, ObjectName:string; OpenMode: Word; const ObjectPath:string = ''; const Ext:string = ''):Boolean;

    class function S3GetImageObject(const FilePath, ObjectName:string; OpenMode: Word; const Ext:string = ''):Boolean;
    class function S3GetAudioObject(const FilePath, ObjectName:string; OpenMode: Word; const Ext:string = ''):Boolean;
    class function S3GetVideoObject(const FilePath, ObjectName:string; OpenMode: Word; const Ext:string = ''):Boolean;
    class function S3GetFileObject(const FilePath, ObjectName:string; OpenMode: Word; const Ext:string = ''):Boolean;

    class function PostWords(const fltno, baseName, Key:string; const EmployeeNum, FltId:Int64; const Text:string):Boolean;
    class function PostImage(const fltno, baseName, Key:string; const EmployeeNum, FltId:Int64; const ObjectName:string; const Width, Height:Integer):Boolean;
    class function PostVoice(const fltno, baseName, Key:string; const EmployeeNum, FltId:Int64; const ObjectName:string; const Second:Integer):Boolean;
    class function PostVideo(const fltno, baseName, Key:string; const EmployeeNum, FltId:Int64; const ObjectName:string; const Second:Integer):Boolean;

    class function GetFlightUnreadNum(const baseName:string; const MaxNo, FltId: Int64): Int64;
    class function GetRecent(const baseName:string; const FltId:Int64; const recordNum:Integer):string;
    class function GetRequest(const baseName:string; const FltId:Int64; const startNo, endNo:Integer):string;
    class function GetLatest(const baseNames:array of string; const MaxNos, FltIds: array of Int64):string;
    class function GetFilerecord(const baseName:string; const FltId:Int64):string;
  end;

implementation

uses
  Data.Cloud.AmazonAPI, Data.Cloud.CloudAPI, uAWSRestServiceUnit, Md5, System.JSON, uCommonTypesUnit;

const
//  GAWSFOSSAPIID = '0jkdii0jd5';
//  GAWSFOSSS3_BucketName = 'g5air-foss-mnt-im';
  GAWSFOSSAPIID = 'ng9523s2wh';
  GAWSFOSSS3_BucketName = 'hx-foss';
  GAWSFOSSS3_ImagePath = 'images/';
  GAWSFOSSS3_AudioPath = 'audio/';
  GAWSFOSSS3_VideoPath = 'video/';
  GAWSFOSSS3_FilePath = 'files/';
  GAWSFOSSAPI_unreadNum = '/formal/message/unreadNum';  //未读条数    Get
  GAWSFOSSAPI_PostWords = '/formal/message/postwords';  //发送文字    Post
  GAWSFOSSAPI_PostImage = '/formal/message/postimage';  //发送图片    Post
  GAWSFOSSAPI_PostAudio = '/formal/message/postaudio';  //发送语音    POST
  GAWSFOSSAPI_PostVideo = '/formal/message/postvideo';  //发送语音    POST
  GAWSFOSSAPI_Recent = '/formal/message/recent';      //最新消息    Get
  GAWSFOSSAPI_Request = '/formal/message/request';    //最新消息    Get
  GAWSFOSSAPI_Latest = '/formal/message/latest';      //最后一条消息    Get
  GAWSFOSSAPI_Filerecord = '/formal/message/filerecord'; //获取附件    Get
{ TAWSChatService }

class function TAWSChatService.GetFilerecord(const baseName: string;
  const FltId: Int64): string;
var
  XML:string;
  ResponseInfo: TCloudResponseInfo;
  Parame:TStringList;
begin
  //请求 最近指定条数的消息记录
  Result:='';
  Parame:=TStringList.Create;
  ResponseInfo:=TCloudResponseInfo.Create;
  try
    Parame.Values['baseName']:=baseName;
    Parame.Values['fltid']:=InttoStr(FltId);
    XML:=GAWSRestService.GateWay.GetXML(GAWSFOSSAPIID, GAWSFOSSAPI_Filerecord, Parame, ResponseInfo);
    if ResponseInfo.StatusCode = 200 then
      Result:=XML;
  finally
    ResponseInfo.Free;
    Parame.Free;
  end;

end;

class function TAWSChatService.GetFlightUnreadNum(const baseName: string;
  const MaxNo, FltId: Int64): Int64;
var
  XML:string;
  Parame:TStringList;
  ResponseInfo:TCloudResponseInfo;
begin
  //请求 每个航班未读条数
  Result:=0;
  Parame:=TStringList.Create;
  ResponseInfo:=TCloudResponseInfo.Create;
  try
    Parame.Values['baseName']:=baseName;
    Parame.Values['maxNo']:=InttoStr(MaxNo);
    Parame.Values['fltid']:=InttoStr(FltId);
    XML:=GAWSRestService.GateWay.GetXML(GAWSFOSSAPIID, GAWSFOSSAPI_unreadNum, Parame, ResponseInfo);
    if ResponseInfo.StatusCode = 200 then
    begin
      TryStrToInt64(XML, Result);
    end;

  finally
    ResponseInfo.Free;
    Parame.Free;
  end;

end;

class function TAWSChatService.GetLatest(const baseNames: array of string;
  const MaxNos, FltIds: array of Int64): string;
var
  i:Integer;
  XML:string;
  ResponseInfo: TCloudResponseInfo;
  FlightItem:TJSONObject;
  Parame:TJSONArray;
begin
  Result:='';
  if Length(baseNames) = 0 then exit;
  if Length(baseNames) <> Length(MaxNos) then exit;
  if Length(baseNames) <> Length(FltIds) then exit;

  Parame:=TJSONArray.Create;
  ResponseInfo:=TCloudResponseInfo.Create;
  try
    for i := 0 to Length(baseNames) - 1 do
    begin
      FlightItem:=TJSONObject.Create;
      FlightItem.AddPair('baseName', TJSONString.Create(baseNames[i]));
      FlightItem.AddPair('fltid', TJSONNumber.Create(fltids[i]));
      FlightItem.AddPair('maxNo', TJSONNumber.Create(maxNos[i]));

      Parame.AddElement(FlightItem);
    end;
    XML:=GAWSRestService.GateWay.PostXML(GAWSFOSSAPIID, GAWSFOSSAPI_Latest, nil, Parame.ToJSON, ResponseInfo);
    if ResponseInfo.StatusCode = 200 then
      Result:=XML;
  finally
    ResponseInfo.Free;
    Parame.Free;
  end;
end;

class function TAWSChatService.GetRecent(const baseName: string;
  const FltId: Int64; const recordNum: Integer): string;
var
  XML:string;
  ResponseInfo: TCloudResponseInfo;
  Parame:TStringList;
begin
  //请求 最近指定条数的消息记录
  Result:='';
  Parame:=TStringList.Create;
  ResponseInfo:=TCloudResponseInfo.Create;
  try
    Parame.Values['baseName']:=baseName;
    Parame.Values['fltid']:=InttoStr(FltId);
    Parame.Values['recordNum']:=InttoStr(recordNum);
    XML:=GAWSRestService.GateWay.GetXML(GAWSFOSSAPIID, GAWSFOSSAPI_Recent, Parame, ResponseInfo);
    if ResponseInfo.StatusCode = 200 then
      Result:=XML;
  finally
    ResponseInfo.Free;
    Parame.Free;
  end;
end;

class function TAWSChatService.GetRequest(const baseName: string;
  const FltId: Int64; const startNo, endNo: Integer): string;
var
  XML:string;
  ResponseInfo: TCloudResponseInfo;
  Parame:TStringList;
begin
  //请求 最近指定条数的消息记录
  Result:='';
  Parame:=TStringList.Create;
  ResponseInfo:=TCloudResponseInfo.Create;
  try
    Parame.Values['baseName']:=baseName;
    Parame.Values['fltid']:=InttoStr(FltId);
    Parame.Values['startNo']:=InttoStr(startNo);
    Parame.Values['endNo']:=InttoStr(endNo);
    XML:=GAWSRestService.GateWay.GetXML(GAWSFOSSAPIID, GAWSFOSSAPI_Request, Parame, ResponseInfo);
    if ResponseInfo.StatusCode = 200 then
      Result:=XML;
  finally
    ResponseInfo.Free;
    Parame.Free;
  end;
end;

class function TAWSChatService.S3GetAudioObject(const FilePath,
  ObjectName: string; OpenMode: Word; const Ext: string): Boolean;
begin
  Result:=S3GetObject(FilePath, ObjectName, OpenMode, GAWSFOSSS3_AudioPath);
end;

class function TAWSChatService.S3GetFileObject(const FilePath,
  ObjectName: string; OpenMode: Word; const Ext: string): Boolean;
begin
  Result:=S3GetObject(FilePath, ObjectName, OpenMode, GAWSFOSSS3_FilePath);
end;

class function TAWSChatService.S3GetImageObject(const FilePath,
  ObjectName: string; OpenMode: Word; const Ext: string): Boolean;
begin
  Result:=S3GetObject(FilePath, ObjectName, OpenMode, GAWSFOSSS3_ImagePath);
end;

class function TAWSChatService.S3GetObject(const FilePath, ObjectName:string; OpenMode: Word;
  const ObjectPath:string = ''; const Ext:string = ''): Boolean;
var
  FullPath:string;
  ObjectStream:TFileStream;
begin
  ObjectStream := nil;
  Result:=False;
  try
    try
      FullPath:=FilePath + ObjectName + Ext;
      ObjectStream := TFileStream.Create(FullPath, OpenMode);
      Result := GAWSRestService.S3.GetObject(GAWSFOSSS3_BucketName, ObjectPath + ObjectName, ObjectStream);
    except
    end;
  finally
    ObjectStream.Free;
    if not Result then
      DeleteFile(FullPath);
  end;

end;

class function TAWSChatService.S3GetVideoObject(const FilePath,
  ObjectName: string; OpenMode: Word; const Ext: string): Boolean;
begin
  Result:=S3GetObject(FilePath, ObjectName, OpenMode, GAWSFOSSS3_VideoPath);
end;

class function TAWSChatService.S3UpImageObject(const BitMap: TBitMap; const SaveParams: PBitmapCodecSaveParams = nil;
  const SavePath:string = ''; const Ext:string = ''): string;
var
  Stream:TMemoryStream;
begin
  if BitMap = nil then exit;
  try
    Stream:=TMemoryStream.Create;
    SaveBitMapToStream(BitMap, Stream, SaveParams);
    Stream.Position:=0;
    Result:=S3UpObject(Stream, SavePath, GAWSFOSSS3_ImagePath, Ext);
  finally
    Stream.Free;
  end;
end;

class function TAWSChatService.S3UpObject(const Buffer: TBytes; ObjectName: string;
  const SavePath:string = ''; const ObjectPath:string = ''; const Ext:string = ''):Boolean;
var
  SaveStream:TFileStream;
begin
  Result:=GAWSRestService.S3.UploadObject(GAWSFOSSS3_BucketName, ObjectPath + ObjectName + Ext, Buffer, false, nil, nil, amzbaPublicRead, nil);
  if Result and (SavePath<>'') then
  begin
    SaveStream:=TFileStream.Create(SavePath + ObjectName + Ext, fmCreate);
    try
      if SaveStream<>nil then
      begin
        if SaveStream<>nil then
          SaveStream.WriteBuffer(Buffer[0], Length(Buffer));
      end;
    finally
      SaveStream.Free;
    end;
  end;
end;

class function TAWSChatService.S3UpAudoiObject(const Stream:TFileStream;
  const SavePath:string = ''; const Ext:string = ''): string;
var
	FileHandle: THandle;
	MapHandle: THandle;
	ViewPointer: pointer;
	Context: MD5Context;
  ABuf:TBytes;
  AResult : MD5Digest;
  ObjectName:string;
begin
  Result:='';
  if Stream = nil then exit;
	MD5Init(Context);
  try
    SetLength(ABuf, Stream.Size);
    Stream.Seek(0,TSeekOrigin.soBeginning);
    Stream.ReadBuffer(ABuf[0],Stream.Size);
    ViewPointer := ABuf;
    try
      MD5Update(Context, ViewPointer, Stream.Size);
    finally
    end;
    MD5Final(Context, AResult);
    ObjectName := MD5Print(AResult);
    if S3UpObject(ABuf, ObjectName, SavePath, GAWSFOSSS3_AudioPath, Ext) then
      Result := ObjectName + Ext;
  finally
    SetLength(ABuf,0);
  end;
end;

class function TAWSChatService.S3UpVideoObject(const Stream:TFileStream;
  const SavePath:string = ''; const Ext:string = ''): string;
var
	FileHandle: THandle;
	MapHandle: THandle;
	ViewPointer: pointer;
	Context: MD5Context;
  ABuf:TBytes;
  AResult : MD5Digest;
  ObjectName:string;
begin
  Result:='';
  if Stream = nil then exit;
	MD5Init(Context);
  try
    SetLength(ABuf,Stream.Size);
    Stream.Seek(0,TSeekOrigin.soBeginning);
    Stream.ReadBuffer(ABuf[0],Stream.Size);
    ViewPointer := ABuf;
    try
      MD5Update(Context, ViewPointer, Stream.Size);
    finally
    end;
    MD5Final(Context, AResult);
    ObjectName := MD5Print(AResult);
    if S3UpObject(ABuf, ObjectName, SavePath, GAWSFOSSS3_VideoPath, Ext) then
      Result:=ObjectName;
  finally
    SetLength(ABuf,0);
  end;

end;

class function TAWSChatService.S3UpObject(const Stream:TStream; const SavePath:string = '';
  const ObjectPath:string = ''; const Ext:string = ''):string;
var
  ObjectName:string;
  ResponseInfo: TCloudResponseInfo;
  ViewPointer:Pointer;
	Context: MD5Context;
  ABuf:TBytes;
  AResult : MD5Digest;
begin
  Result:='';
  MD5Init(Context);
  try
    SetLength(ABuf, Stream.Size);
    Stream.Seek(0,TSeekOrigin.soBeginning);
    Stream.ReadBuffer(ABuf[0], Stream.Size);
    ViewPointer := ABuf;
    try
      MD5Update(Context, ViewPointer, Stream.Size);
    finally
    end;
    MD5Final(Context, AResult);
    ObjectName :=  MD5Print(AResult);
    if S3UpObject(ABuf, ObjectName, SavePath, ObjectPath, Ext) then
      Result:=ObjectName + Ext;
  finally
    SetLength(ABuf,0);
  end;

end;

class function TAWSChatService.S3UpObject(const FilePath:string; const SavePath:string = '';
  const ObjectPath:string = ''; const Ext:string = ''): string;
var
  Stream:TFileStream;
begin
  Stream := TFileStream.Create(FilePath, $0000); //fmOpenRead
  try
    Result:=S3UpObject(Stream, SavePath, ObjectPath, Ext);
  finally
    Stream.Free;
  end;
end;

class function TAWSChatService.PostImage(const fltno, baseName, Key: string;
  const EmployeeNum, FltId: Int64; const ObjectName:string; const Width, Height:Integer): Boolean;
var
  XML:string;
  Parame:TJSONObject;
  Content:TJSONObject;
  ResponseInfo:TCloudResponseInfo;
begin
  //发送图片信息
  Result:=False;
  if ObjectName<>'' then
  begin
    Parame:=TJSONObject.Create;
    Content:=TJSONObject.Create;
    ResponseInfo:=TCloudResponseInfo.Create;
    try
      Parame.AddPair('fltno', TJSONString.Create(fltno));
      Parame.AddPair('baseName', TJSONString.Create(baseName));
      Parame.AddPair('employeeNum', TJSONNumber.Create(employeeNum));
      Parame.AddPair('fltid', TJSONNumber.Create(fltid));
      Parame.AddPair('key', TJSONString.Create(Key));

      Content.AddPair('width', TJSONString.Create(InttoStr(Width)));
      Content.AddPair('height', TJSONString.Create(InttoStr(Height)));
      Content.AddPair('filename', TJSONString.Create(ObjectName));
      Parame.AddPair('content', Content);

      XML:=GAWSRestService.GateWay.PostXML(GAWSFOSSAPIID, GAWSFOSSAPI_PostImage, nil, Parame.ToJSON, ResponseInfo);
      if ResponseInfo.StatusCode = 200 then
        Result:=True;
    finally
      ResponseInfo.Free;
      Parame.Free;
    end;
  end;
end;

class function TAWSChatService.PostVideo(const fltno, baseName, Key: string;
  const EmployeeNum, FltId: Int64; const ObjectName: string; const Second:Integer): Boolean;
var
  XML:string;
  Parame:TJSONObject;
  Content:TJSONObject;
  ResponseInfo:TCloudResponseInfo;
begin
  //发送视频信息
  Result:=False;
  if ObjectName<>'' then
  begin
    Parame:=TJSONObject.Create;
    Content:=TJSONObject.Create;
    ResponseInfo:=TCloudResponseInfo.Create;
    try
      Parame.AddPair('fltno', TJSONString.Create(fltno));
      Parame.AddPair('baseName', TJSONString.Create(baseName));
      Parame.AddPair('employeeNum', TJSONNumber.Create(employeeNum));
      Parame.AddPair('fltid', TJSONNumber.Create(fltid));
      Parame.AddPair('fileName', TJSONString.Create(ObjectName));
      Parame.AddPair('key', TJSONString.Create(Key));


      Parame.AddPair('fltno', TJSONString.Create(fltno));
      Parame.AddPair('baseName', TJSONString.Create(baseName));
      Parame.AddPair('employeeNum', TJSONNumber.Create(employeeNum));
      Parame.AddPair('fltid', TJSONNumber.Create(fltid));
      Parame.AddPair('key', TJSONString.Create(Key));

      Content.AddPair('second', TJSONString.Create(InttoStr(Second)));
      Content.AddPair('filename', TJSONString.Create(ObjectName));
      Parame.AddPair('content', Content);

      XML:=GAWSRestService.GateWay.PostXML(GAWSFOSSAPIID, GAWSFOSSAPI_PostVideo, nil, Parame.ToJSON, ResponseInfo);
      if ResponseInfo.StatusCode = 200 then
        Result:=True;
    finally
      ResponseInfo.Free;
      Parame.Free;
    end;
  end;

end;

class function TAWSChatService.PostVoice(const fltno, baseName, Key: string;
  const EmployeeNum, FltId: Int64; const ObjectName: string; const Second:Integer): Boolean;
var
  XML:string;
  Parame:TJSONObject;
  Content:TJSONObject;
  ResponseInfo:TCloudResponseInfo;
begin
  //发送语音信息
  Result:=False;
  if ObjectName<>'' then
  begin
    Parame:=TJSONObject.Create;
    Content:=TJSONObject.Create;
    ResponseInfo:=TCloudResponseInfo.Create;
    try
      Parame.AddPair('fltno', TJSONString.Create(fltno));
      Parame.AddPair('baseName', TJSONString.Create(baseName));
      Parame.AddPair('employeeNum', TJSONNumber.Create(employeeNum));
      Parame.AddPair('fltid', TJSONNumber.Create(fltid));
      Parame.AddPair('key', TJSONString.Create(Key));

      Content.AddPair('second', TJSONString.Create(InttoStr(Second)));
      Content.AddPair('filename', TJSONString.Create(ObjectName));
      Parame.AddPair('content', Content);

      XML:=GAWSRestService.GateWay.PostXML(GAWSFOSSAPIID, GAWSFOSSAPI_PostAudio,nil, Parame.ToJSON, ResponseInfo);
      if ResponseInfo.StatusCode = 200 then
        Result:=True;
    finally
      ResponseInfo.Free;
      Parame.Free;
    end;
  end;

end;

class function TAWSChatService.PostWords(const fltno, baseName, Key: string;
  const EmployeeNum, FltId: Int64; const Text:string): Boolean;
var
  XML:string;
  Parame:TJSONObject;
  Content:TJSONObject;
  ResponseInfo:TCloudResponseInfo;
begin
  //发送文字信息
  Result:=False;
  Parame:=TJSONObject.Create;
  Content:=TJSONObject.Create;
  ResponseInfo:=TCloudResponseInfo.Create;
  try
    Parame.AddPair('fltno', TJSONString.Create(fltno));
    Parame.AddPair('baseName', TJSONString.Create(baseName));
    Parame.AddPair('employeeNum', TJSONNumber.Create(employeeNum));
    Parame.AddPair('fltid', TJSONNumber.Create(FltId));
    Parame.AddPair('key', TJSONString.Create(Key));

    Content.AddPair('text', TJSONString.Create(Text));
    Parame.AddPair('content', Content);

    XML:=GAWSRestService.GateWay.PostXML(GAWSFOSSAPIID, GAWSFOSSAPI_PostWords, nil, Parame.ToJSON, ResponseInfo);

    if ResponseInfo.StatusCode = 200 then
      Result:=True;
  finally
    ResponseInfo.Free;
    Parame.Free;
  end;

end;

end.
