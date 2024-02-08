Attribute VB_Name = "FuncModule"
'чтение файлов с папки и выборка подходящих условию
Function WriteAndFilterFile() As Boolean

Dim wsMain, wsSettings As Worksheet

Set wsMain = ThisWorkbook.Sheets("Главная")
Set wsSettings = ThisWorkbook.Sheets("Настройки")
WriteAndFilterFile = False

Dim fso, myPath, myFolder, myFile, myFiles(), i

If wsMain.Cells(3, "H") = "1" Then
    myPath = ThisWorkbook.Path & Application.PathSeparator
Else:
    myPath = wsMain.Cells(1, "C")
End If

wsSettings.Cells(2, "B") = wsMain.Cells(4, "H")

Set fso = CreateObject("Scripting.FileSystemObject")
On Error Resume Next: Set myFolder = fso.GetFolder(myPath)
If Not curfold Is Nothing Then
    If myFolder.Files.Count = 0 Then
        MsgBox "Проверьте правильность ввода папки"
        Exit Function
    End If
    
    ReDim myFiles(1 To myFolder.Files.Count)
        For Each myFile In myFolder.Files
            i = i + 1
            myFiles(i) = myFile.Name
        Next
    
    FindNameStr = wsSettings.Cells(2, "B")
    
    wsMain.Range("B3:F100").Clear
    wsSettings.Range("A2:A100").Clear
    
    k = 2
    mk = 3
    
    For j = 1 To i
        If InStr(1, UCase(myFiles(j)), FindNameStr, vbTextCompare) > 0 Then
            wsSettings.Cells(k, "A") = myFiles(j)
            wsMain.Cells(mk, "B") = myFiles(j)
            k = k + 1
            mk = mk + 1
            WriteAndFilterFile = True
        End If
    Next j

Else:
    MsgBox "Проверьте правильность ввода папки"
End If

End Function
Sub StopApp()
Application.ScreenUpdating = False
Application.Calculation = xlCalculationAutomatic
End Sub
Sub StartApp()
Application.EnableEvents = True
Application.ScreenUpdating = True
Application.Calculation = xlCalculationAutomatic
End Sub

'Функция поиска строки
Function FindName(Name As String, Sht As Worksheet, Col As Integer, Row As Integer) As Integer

CntStr = Sht.Cells(Rows.Count, Col).End(xlUp).Row

FindName = 0

For i = Row To CntStr
    If Sht.Cells(i, Col) = Name Then
        FindName = i
        Exit For
    End If
Next i

End Function

'Функция поиска нужного столбца
Function FindNumberCol(Name As String, Sht As Worksheet, Col As Integer, Row As Integer) As Integer

CntCol = Sht.Cells(Row, Sht.Columns.Count).End(xlToLeft).Column

FindNumberCol = 0

For i = Col To CntCol
    If Sht.Cells(Row, i) = Name Then
        FindNumberCol = i
        Exit For
    End If
Next i

End Function
'функция получения буквы столбца
Function ColumnName(ByVal Col As Long) As String
   On Error Resume Next
   ColumnName = Application.ConvertFormula("r1c" & Col, xlR1C1, xlA1)
   ColumnName = Replace(Replace(Mid(ColumnName, 2), "$", ""), "1", "")
End Function

'Функция поиска индекса минимального значения
Function MinValCompany(Price As Double, Rws As Integer) As String
Dim wsCorrectPrice As Worksheet

Set wsCorrectPrice = ThisWorkbook.Sheets("Согласование цен")
MinValCompany = ""

For i = 6 To 10
        If wsCorrectPrice.Cells(Rws, ColumnName(i)) = Price Then
            MinValCompany = wsCorrectPrice.Cells(3, ColumnName(i))
            Exit For
        End If
Next i

End Function


Sub ForVprLimitsPrice()
Dim wsTmp As Worksheet
Set wsTmp = ThisWorkbook.Sheets("Данные обработанные")

CntStrData = wsTmp.Cells(Rows.Count, 1).End(xlUp).Row
wsTmp.Cells(2, "V").FormulaLocal = "=A2&Q2"
wsTmp.Cells(2, "W").FormulaLocal = "=H2"
wsTmp.Select
wsTmp.Range("V2:W2").AutoFill Destination:=Range("V2:W" & CStr(CntStrData))
 
End Sub

Sub ForVprLimitsPriceDel()
Dim wsTmp As Worksheet
Set wsTmp = ThisWorkbook.Sheets("Данные обработанные")

wsTmp.Columns("V:W").ClearContents
End Sub

'Забираем координаты Яндекса
Sub GetYandexTravel()
Dim wsDataReport As Worksheet
Dim html As HTMLDocument: Set html = New HTMLDocument

Set wsDataReports = ThisWorkbook.Sheets("Сформированная таблица")

CntStr = wsDataReports.Cells(Rows.Count, 1).End(xlUp).Row


Dim sURIBegin, sURLMedium, sURLEnd, sHTMLlink, GetText As String
'sURI = "https://www.avtodispetcher.ru/distance/?from=52.008029+55.766228&to=53.122198+56.68999&v=&vt=car&rm=110&rp=90&rs=60&ru=40&fc=8&fp=42&ov=&atn=&aup=&atr=&afd=&ab=&acb="
sURL = "https://www.avtodispetcher.ru/distance/?from="
sURLMedium = "&to="
sURLEnd = "&v=&vt=car&rm=110&rp=90&rs=60&ru=40&fc=8&fp=42&ov=&atn=&aup=&atr=&afd=&ab=&acb="
  
For i = 2 To CntStr
    sHTMLlink = sURL & Replace(CStr(wsDataReports.Cells(i, "D")), ",", ".") & "+" & Replace(CStr(wsDataReports.Cells(i, "E")), ",", ".") & sURLMedium
    sHTMLlink = sHTMLlink & Replace(CStr(wsDataReports.Cells(i, "K")), ",", ".") & "+" & Replace(CStr(wsDataReports.Cells(i, "E")), ",", ".") & sURLEnd
    GetText = GetHTTPResponse(sHTMLlink)

    If InStr(GetText, "totalDistance") > 0 Then
        tmp = Mid(GetText, InStr(GetText, "totalDistance"), 35)
        wsDataReports.Cells(i, "M") = Mid(tmp, 16, InStr(tmp, "<") - 16)
    Else:
        wsDataReports.Cells(i, "M") = "Не удалось определить"
    End If
    
    If InStr(GetText, "totalDistance") > 0 Then
        tmp = Mid(GetText, InStr(GetText, "totalTime"), 20)
        wsDataReports.Cells(i, "N") = Mid(tmp, 12, InStr(tmp, "<") - 12)
    Else:
        wsDataReports.Cells(i, "N") = "Не удалось определить"
    End If
    
    wsDataReports.Cells(i, "P").FormulaLocal = "=F" & CStr(i) & "+G" & CStr(i) & "+H" & CStr(i) & "+N" & CStr(i) & ""
    wsDataReports.Cells(i, "Q").FormulaLocal = "=ЕСЛИОШИБКА(ЕСЛИ(P" & CStr(i) & ">" & "O" & CStr(i) & ";  " & """ " & "Опоздаем" & """" & ";" & """" & "Идем по графику" & """" & ");" & """" & "Не удалось определить" & """" & ")"
    
 Next i

MsgBox ("Готово")
End Sub

Sub CreateEmail()
Dim wsEmail, wsData, wsDataReport, wsSettings As Worksheet

Set wsDataReports = ThisWorkbook.Sheets("Сформированная таблица")
Set wsEmail = ThisWorkbook.Sheets("Письмо")
Set wsSettings = ThisWorkbook.Sheets("Настройки")

CntStr = wsDataReports.Cells(Rows.Count, 1).End(xlUp).Row
CntStrEmail = 2
wsEmail.Cells.Clear

wsSettings.Range("E:T").Copy
wsEmail.Cells(1, "A").PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks _
        :=False, Transpose:=False
        
For i = 2 To CntStr
    If wsDataReports.Cells(i, "Q").Value = " Опоздаем" Then
        wsDataReports.Range("A" & CStr(i) & ":P" & CStr(i)).Copy
        wsEmail.Cells(CntStrEmail, "A").PasteSpecial Paste:=xlPasteValues, Operation:=xlNone, SkipBlanks:=False, Transpose:=False
        CntStrEmail = CntStrEmail + 1
    End If
Next i

wsEmail.Columns("F:F").NumberFormat = "m/d/yyyy"
wsEmail.Columns("G:G").NumberFormat = "[$-x-systime]h:mm:ss AM/PM"
wsEmail.Columns("H:H").NumberFormat = "[$-x-systime]h:mm:ss AM/PM"
wsEmail.Columns("I:I").NumberFormat = "m/d/yyyy"
wsEmail.Columns("J:J").NumberFormat = "[$-x-systime]h:mm:ss AM/PM"
wsEmail.Columns("N:N").NumberFormat = "[$-x-systime]h:mm:ss AM/PM"
wsEmail.Columns("O:O").NumberFormat = "[$-x-sysdate]dddd, mmmm dd, yyyy"
wsEmail.Columns("O:P").NumberFormat = "dd/mm/yy h:mm;@"

MsgBox ("Готово")
End Sub


Public Function GetHTTPResponse(ByVal sURL As String) As String
    On Error Resume Next
    Set oXMLHTTP = CreateObject("MSXML2.XMLHTTP")
    With oXMLHTTP
        .Open "GET", sURL, False
       ' uncomment and fill in IP and username/password
       ' if you use proxy
       ' .setProxy 2, "192.168.100.1:3128"
       ' .setProxyCredentials "user", "password"
       .setOption "SXH_OPTION_IGNORE_SERVER_SSL_CERT_ERROR_FLAGS", "SXH_SERVER_CERT_IGNORE_ALL_SERVER_ERRORS"
       .send
        GetHTTPResponse = .responseText
    End With
    Set oXMLHTTP = Nothing
End Function




Sub Distance(lat1, lng1, lat2, lng2)
Const pi = 3.14159265358979 ' определяем константу pi
'lat1 = 55.766228 ' Широта А
'lng1 = 52.008029  ' Долгота А
'lat2 = 56.68999 ' Широта Б
'lng2 = 53.122198 ' Долгота Б
' переводим градусы в радианы
GradToRadLat1 = lat1 * pi / 180 ' Радианы Широты А
GradToRadLng1 = lng1 * pi / 180 ' Радианы Долготы А
GradToRadLat2 = lat2 * pi / 180 ' Радианы Широты Б
GradToRadLng2 = lng2 * pi / 180 ' Радианы Долготы Б
a = 6378137 ' экваториальный радиус земли (метров)
f = 1 / 298.257223563 ' сжатие
b = a * (1 - f) ' полярный радиус
GSinLat = (Sin((GradToRadLat1 - GradToRadLat2) / 2) ^ 2) 'Гаверсинус широты
GSinLng = (Sin((GradToRadLng1 - GradToRadLng2) / 2) ^ 2) 'Гаверсинус долготы
CosLat = Cos(GradToRadLat1) * Cos(GradToRadLat2) 'Произведение косинусов широт
'вычисление арксинуса угла (Sqr(GSinLat + GSinLng * CosLat)
x = Sqr(GSinLat + GSinLng * CosLat)
Arcsin = Atn(x / Sqr(-x * x + 1))
'расчет дистанции
dist = (a + b) * Arcsin
MsgBox dist
End Sub

Function ExportHTML() As String
Dim wsEmail  As Worksheet
 
Set wsEmail = ThisWorkbook.Sheets("Письмо")

    On Error Resume Next
    Selection.Areas(1).Select

    iFirstLine = 1
    iFirstCol = 1
    iLastLine = wsEmail.Cells(Rows.Count, 1).End(xlUp).Row
    iLastCol = 16
 
    sTableClass = "ExcelTable"
    sOddRowClass = "odd"
 
    SOutput = "<div><table class='" & sTableClass & "' border=1 width=500px align=center>"
    For k = iFirstLine To iLastLine
        If (k \ 2 <> k / 2) Then
            sLine = "<tr class ='" & sOddRowClass & "'>"
        Else
            sLine = "<tr>"
        End If
 
        iCountColspan = 0
        For j = iFirstCol To iLastCol
 
            If Cells(k, j).MergeCells = True Then
 
                iCountColspan = Cells(k, j).MergeArea.Count
            Else
                iCountColspan = 0
            End If
            Set oCurrentCell = ActiveSheet.Cells(k, j)
            sLine = sLine & "<td"
 
            If iCountColspan > 1 Then
                sLine = sLine & " colspan=" & iCountColspan
                j = j + iCountColspan - 1
                iCountColspan = 0
            End If
 
            If oCurrentCell.HorizontalAlignment = -4108 Then sLine = sLine & " style='text-align: center;'"
            sLine = sLine & ">"
 
            If oCurrentCell.Text <> "" Then sValue = oCurrentCell.Text Else sValue = "&nbsp;"
 
            If oCurrentCell.Font.Bold = True Then sValue = "<b>" & sValue & "</b>"
 
            If oCurrentCell.Font.Italic = True Then sValue = "<i>" & sValue & "</i>"
 
            sLine = sLine & sValue & "</td>"
            If k = iFirstLine Then sLine = Replace(sLine, "<td", "<th")
 
        Next j
        SOutput = SOutput & sLine & "</tr>"
    Next k
 
    SOutput = SOutput & "</table></div>"
 
    ExportHTML = SOutput
    
End Function


