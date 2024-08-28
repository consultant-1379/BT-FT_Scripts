#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $date_time_check = "true";
my $result;
my $fail_result = 0;
my $pass_result = 0;
my $result_no;
my $empty_num = 0;

my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
open(Pass, "<Passwords.txt");
	my $name = do {local $/; <Pass> };
	my ($DCPass,$DwhrepPass,$EtlrepPass) = split("\n",$name);
	#print "DCPass: $DCPass , DwhrepPass: $DwhrepPass , EtlrepPass: $DwhrepPass";
##################################################################################
#             The DATETIME value for the FILENAME of the HTML LOGS               #
##################################################################################

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
  $mon++;
  $year=1900+$year;
my $datenew =sprintf("%4d%02d%02d%02d%02d%02d",$year,$mon,$mday,$hour,$min,$wday);

############################################################
# GET TIMESTAMP
# This is a utility 
sub getTime{
  my ($sec,$min,$hour,$mday,$mon,$year,$wday, $yday,$isdst)=localtime(time);
  return sprintf "%4d-%02d-%02d %02d:%02d:%02d\n", $year+1900,$mon+1,$mday,$hour,$min,$sec;
}

############################################################
# GET THE HTML HEADER
# This is a utility for the log output file in HTML 
sub getHtmlHeader{
my $testCase = shift;
if ($testCase eq "")
{
return qq{<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<title>
ENIQ Regression Feature Test
</title>
<STYLE TYPE="text/css">
h3{font-family:tahoma;font-size:12px}
body,td,tr,p,h{font-family:tahoma;font-size:11px}
.pre{font-family:Courier;font-size:9px;color:#000}
.h{font-size:9px}
.td{font-size:9px}
.tr{font-size:9px}
.h{color:#3366cc}
.q{color:#00c}

</STYLE>
</head>
<body>
};
}
else
{
return qq{<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<title>
$testCase
</title>
<STYLE TYPE="text/css">
h3{font-family:tahoma;font-size:12px}
body,td,tr,p,h{font-family:tahoma;font-size:11px}
.pre{font-family:Courier;font-size:9px;color:#000}
.h{font-size:9px}
.td{font-size:9px}
.tr{font-size:9px}
.h{color:#3366cc}
.q{color:#00c}

</STYLE>
</head>
<body>
};
}
}


############################################################
# GET HTML TAIL
# This is a utility for the log output file in HTML 

sub getHtmlTail{
return qq{
</table>
<br>
</body>
</html>
};

}
############################################################
# WRITE HTML
# This is a utility for the log output file in HTML 

sub writeHtml{
my $server = shift;
my $out    = shift;
open(OUT," > $LOGPATH/$server.html");
print OUT $out;
close(OUT);
return "$LOGPATH/$server.html\n";
}

############################################################
# ExecuteSQL
# This will give the resultset data for the queries passed

sub executeSQL{

        my $dbname = $_[0];
        my $port = $_[1];
        my $cre = $_[2];
		my $cre1 = $_[3];
        my $arg = $_[4];
        my $type = $_[5];
        #print "executeSQL : $arg  $type $cre $port $dbname\n\n";
                
		my $connstr = "ENG=$dbname;CommLinks=tcpip{host=localhost;port=$port};UID=$cre;PWD=$cre1";
		#print $connstr;
				my $dbh = DBI->connect( "DBI:SQLAnywhere:$connstr", '', '', {AutoCommit => 1} ) or warn $DBI::errstr;
                my $sel_stmt=$dbh->prepare($arg) or warn $DBI::errstr;

        if ( $type eq "ROW" ) {
            $sel_stmt->execute() or warn $DBI::errstr;
                my @result = $sel_stmt->fetchrow_array();
                $sel_stmt->finish();
                #$dbh->disconnect;
                return @result;
                }       
        elsif ( $type eq "ALL" ) {
                $sel_stmt->execute() or warn $DBI::errstr;
                my $result = $sel_stmt->fetchall_arrayref();
                $sel_stmt->finish();
                return $result;
        }
        $dbh->disconnect;
}

##################################################################################################################################
																							
sub uniq {
			return keys  %{{ map { $_ => 1 } @_ }};
	}
##################################################################################################################################
sub date_time_check {

my @final_table_name = ();

my $file = 'data_new.txt';
open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
my @array = <FILE>; 
my $line = ();
$line = shift @array;
chomp $line;
#print "$line\n";
	
my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNameKey) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.+)/;
@array = split(",",$Nodename);
$Nodename = $array[0];
my ($datetime_year,$hyphen1,$datetime_month,$hyphen2,$datetime_date,$space,$datetime_hour,$colon1,$datetime_min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;

my $date_for_filename = $datetime_year.$datetime_month.$datetime_date;
my $time_for_filename = $datetime_hour.$datetime_min;
 if($tp_name eq "DC_E_ERBS" or $tp_name eq "DC_E_RBS" or $tp_name eq "DC_E_RAN" or $tp_name eq "DC_E_CPP"){
	#ERBS & RBS & RAN
	#$time_for_filename = $datetime_hour+int(-2).$datetime_min;
	if(int($datetime_hour-2) < 10){
		$time_for_filename="0".(int($datetime_hour-2))."$datetime_min";}
		else{
		$time_for_filename=(int($datetime_hour-2))."$datetime_min";}
}																  
my $package = $tp_name.":((".$build."))";

if ($tp_name =~ m/(DC_\w*)/) {

	$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr>";
	$result .= "<tr><td align=left><font color=000000><b>TABLE NAME</b></font></td><td align=left><font color=000000><b>Start Time from file</b></font></td><td align=left><font color=000000><b>End time from file</b></font></td><td align=left><font color=000000><b>Date_Time_ID</b></font></td><td align=left><font color=000000><b>UTC_ID</b></font></td><td align=left><font color=000000><b>Offset</b></font></td><td align=left><font color=000000><b>PERIOD DURATION</b></font></td><td align=left><font color=000000><b>RESULT</b></font></td></tr>";
								
	$result_no.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
	$result_no.= "<tr><td align=left><font size=3px><font color=magenta><b>NO DATA IN TABLE FOR DATE $datetime AND NODE-NAME: $Nodename</b></font></font></td></tr>";

	my $query_from_repdb = "select distinct TYPENAME from MeasurementType where TYPEID like '%".$package."%' and TYPENAME not like '%BH' order by TYPENAME";
	
	my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_repdb,"ALL");
	
	for my $rows ( @$result_from_repdb ) {
						for my $field ( @$rows ) {
								if ( $field !~ m/^dim*/ || $field !~ m/^DIM*/ )
							{
								push @final_table_name, $field;
							}
						}
					}
	
	
	my $query_for_INTF = "select INTERFACENAME from  InterfaceTechpacks where TECHPACKNAME LIKE '".$tp_name."'";
	
	$result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_for_INTF,"ALL");
	for my $r ( @$result_from_repdb ) {
		for my $f ( @$r ) {
		my $query_for_ID = "select COLLECTION_SET_ID from META_COLLECTION_SETS WHERE COLLECTION_SET_NAME LIKE '".$f."-eniq_oss_1"."'";
	
		$result_from_repdb = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query_for_ID,"ALL");
		for my $ro ( @$result_from_repdb ) {
		for my $fi (@$ro){
		my $query_for_Directory = "select substr(action_contents_01,charindex('processedDir',action_contents_01)+39,charindex('/processed',action_contents_01)-(charindex('processedDir',action_contents_01)+39)) procdir from meta_transfer_actions where collection_set_id = ".$fi."and action_type like 'parse'
";
	
		$result_from_repdb = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query_for_Directory,"ALL");
		for my $row ( @$result_from_repdb ) {
		for my $fie (@$row){
		my $in_path="/eniq/archive/eniq_oss_1/".$fie."/processed";
	#print "final path is $in_path\n";
	
	chdir $in_path or die $!;
	#my $correct_intf_file;
	opendir ( DIR, $in_path ) || die "Error in opening dir due to $! \n";
	while( my $intf_file = readdir(DIR)) {
		
		my ($correct_intf_file) = $intf_file =~ m/(INTF_\w*\_$date_for_filename\.$time_for_filename\.txt)/;
		
		#print "$intf_file\n";
		#print "$correct_intf_file\n";
		if ($intf_file eq $correct_intf_file) {
		
			open INTFILE, '<', $intf_file or die "Could not open '$intf_file', No such file found in the provided path $!\n";
			chomp (my $filename = <INTFILE>);
		
			my $filename_etime;
			#my $time_flag=1;
			my ($na1,$filename_stime,$na3,$dt,$filename_etime_d,$na4);
			
			($na1,$filename_stime,$na3,$filename_etime_d,$na4) = $filename =~ m/(.+\.)(\d+)([\+|\-]\d+\-)(\d+)(.*)/;
			
			my $string_len = length($filename_etime_d);
			
			if($string_len == 4 ){
				#$time_flag = 0;
				$filename_etime = $filename_etime_d;
				#print "if filename_etime:$filename_etime\n";
			}else{
			
				($na1,$filename_stime,$na3,$dt,$filename_etime_d,$na4) = $filename =~ m/(.+\.)(\d+)([\+|\-]\d+\-)(.+\.)(\d+)(.*)/;
				$filename_etime = $filename_etime_d;
				#print "else filename_etime:$filename_etime\n";
			}
			my ($filename_stime_first, $filename_stime_second) = $filename_stime =~ m/(\d\d)(\d\d)/;
			my ($filename_etime_first, $filename_etime_second) = $filename_etime =~ m/(\d\d)(\d\d)/;
				
			my $new_filename_etime = '';
				
			if( $filename_etime_second == '00'){
				
				$new_filename_etime = $filename_stime_first.'60';
				
			}else {
			
				$new_filename_etime = $filename_etime;
			
			}
			
			foreach (@final_table_name){
	
				my $table_name = $_;
				$table_name =~ s/ //g;
				
		
				my $query_from_dwhdb = "select distinct DATETIME_ID,UTC_DATETIME_ID,DC_TIMEZONE,PERIOD_DURATION from ".$table_name."_RAW where DATETIME_ID = '$datetime' and $Nodenamekey = '$Nodename'" ;
		
				my @result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$query_from_dwhdb,"ROW");
		
				if ( !@result_from_dwhdb ) {
				
					$empty_num++;
					
					$result_no.= "<tr><td align=left><font color=000000><b>$table_name</b></font></td></tr>";
					
					}
				else {
		
					my $output_from_dwhdb = join (",",@result_from_dwhdb);
					foreach ($output_from_dwhdb) {
			
						my ($date,$comma1,$utc,$comma2,$offset,$comma3,$period) = $output_from_dwhdb  =~ m/(.+)(,)(.+)(,)(.+)(,)(.+)/;
			
						my ($junk1_date,$hour_date,$junk2_date) = $date =~ m/(.+ )(.+)(:.+:.+)/;
						my ($junk1_utc,$hour_utc,$junk2_utc) = $utc =~ m/(.+ )(.+)(:.+:.+)/;
						my ($hour_offset,$junk2_offset) = $offset =~ m/(...)(.+)/;
			
						if ($hour_utc + $hour_offset == $hour_date){
			
							if ($new_filename_etime - $filename_stime == $period) {
				
								$pass_result++;
					
								$result .= "<tr><td align=left><font color=000000><b>$table_name</b></font></td><td align=left><font color=000000><b>$filename_stime</b></font></td><td align=left><font color=000000><b>$filename_etime</b></font></td><td align=left><font color=000000><b>$date</b></font></td><td align=left><font color=000000><b>$utc</b></font></td><td align=left><font color=000000><b>$offset</b></font></td><td align=left><font color=000000><b>$period</b></font></td><td align=left><font color=006600><b>PASS</b></font></td></tr>";
				
								}
							else {
							
								$fail_result++;
				
								$result .= "<tr><td align=left><font color=000000><b>$table_name</b></font></td><td align=left><font color=000000><b>$filename_stime</b></font></td><td align=left><font color=000000><b>$filename_etime</b></font></td><td align=left><font color=000000><b>$date</b></font></td><td align=left><font color=000000><b>$utc</b></font></td><td align=left><font color=000000><b>$offset</b></font></td><td align=left><font color=000000><b>$period</b></font></td><td align=left><font color=red><b>FAIL</b></font></td></tr>";
				
								}
							}
				
						else{
				
							$fail_result++;
				
							$result .= "<tr><td align=left><font color=000000><b>$table_name</b></font></td><td align=left><font color=000000><b>$filename_stime</b></font></td><td align=left><font color=000000><b>$filename_etime</b></font></td><td align=left><font color=000000><b>$date</b></font></td><td align=left><font color=000000><b>$utc</b></font></td><td align=left><font color=000000><b>$offset</b></font></td><td align=left><font color=000000><b>$period</b></font></td><td align=left><font color=red><b>FAIL</b></font></td></tr>";
				
							}
						}
					}
				}	
			}
		}}}}}}}
closedir(DIR);
$result.=$result_no;
return $result;
	}
else {

	print "$tp_name: This is not a PM TP. This testcase is only for PM TP.\n\n\n";

	}
}
############################################################################################################################
if($date_time_check eq "true")
   {
     print "********Executing DATE- TIME validation Script********\n";

	 
	my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> VERIFY DATE & TIME LOADING </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my $result1=date_time_check();
	    	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($pass_result) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($fail_result)/ <a href=\"#t3\"><font size = 2 color=magenta><b>No Data in table ($empty_num)</td>";
	 print "Date_Check:PASS- $pass_result FAIL- $fail_result No_Data- $empty_num\n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>$result1\t</td>";
	 $report.= "</table>";
	 
     $report.= getHtmlTail();
     my $file = writeHtml("VERIFY_DATE_LOADING_".$datenew,$report);
	 #print "PARTIAL FILE: $file\n"; 
     $date_time_check ="false";
	 
	 
	 print "********Counter Checking Script Test Case is done********\n";
   }
############################################################################################################################