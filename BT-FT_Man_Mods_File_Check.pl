#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;
use XML::Simple;
use File::Basename;


my $verifying_manmods = "true";
my $result;
my $fail_result = 0;
my $pass_result = 0;
my $result_no;
my $empty_num = 0;

#mkdir /eniq/home/dcuser/BT-FT_Log_manmods;
my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
#my $LOGPATH="/eniq/home/dcuser/BT-FT_Log_manmods";
open(Pass, "<Passwords.txt");
	my $name = do {local $/; <Pass> };
	my ($DCPass,$DwhrepPass,$EtlrepPass) = split("\n",$name);
	#print "DCPass: $DCPass , DwhrepPass: $DwhrepPass , EtlrepPass: $DwhrepPass";
##################################################################################
#             The DATETIME value for the FILENAME of the HTML LOGS               #
##################################################################################

my ($sec,$min,$modification_dataour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
  $mon++;
  $year=1900+$year;
my $datenew =sprintf("%4d%02d%02d%02d%02d%02d",$year,$mon,$mday,$modification_dataour,$min,$wday);


############################################################
# GET TIMESTAMP
# This is a utility 
sub getTime{
  my ($sec,$min,$modification_dataour,$mday,$mon,$year,$wday, $yday,$isdst)=localtime(time);
  return sprintf "%4d-%02d-%02d %02d:%02d:%02d\n", $year+1900,$mon+1,$mday,$modification_dataour,$min,$sec;
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
		 elsif ( $type eq "HASH" ) {
                $sel_stmt->execute() or warn $DBI::errstr;
                my $result = $sel_stmt->fetchrow_hashref();
                $sel_stmt->finish();
                return $result;
        }
        $dbh->disconnect;
}

##################################################################################################################################
sub  verifying_manmods{

	print "Please Enter Manual Modification file name:\n";
	my $file_name = <>;
	chomp($file_name);
	print "Entered file name is ",basename($file_name),"\n";	
	#changing the file format Dos to Unix
	system("dos2unix $file_name");
	
	my $file1 = basename($file_name);
	
	# create object
	my $xml = new XML::Simple;
	
	# read XML file
	my $data = $xml->XMLin("$file_name");
	
	my $package ;
	my $techpack_name ;
	my $version;
	my $datetime ;
	my $Nodename ;
	
	my @nameTP =split("_",$file1);
#	if ($file1 =~ m/(DC_\w*)/) {
	if ($nameTP[0] eq "DC" and $nameTP[2] ne "LLE" and $nameTP[2] ne "IPRAN" and $nameTP[2] ne "WLE" and $nameTP[2] ne "FFAXW" ){

		my $file = 'data_new.txt';
		open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
		my @array = <FILE>;
	
		my $line = ();
		$line = shift @array;
		chomp $line;	
		#print "$line\n";
	
		my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNameKey) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.+)/;
	
		$package = $tp_name.":((".$build."))";
		$techpack_name = $tp_name;
		$version = "((".$build."))";
	}else{
		my $time_file = 'time.txt';
		open TIME, '<', $time_file or die "Could not open '$time_file', No such file found in the provided path $!\n";
		my $today = <TIME>;
				
				
		my $file = 'data.txt';
		open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
		my @array = <FILE>;
		my $count = @array;
				
		undef my $line;
	    $line = shift @array;
		chomp $line;
		my ($strings,$r,$b) = $line  =~ m/(\w*_\w*)(\_R.+\_b)(\d+)/;
				
		$package = $strings.":((".$b."))";
		$techpack_name = "$strings";
		$version = "((".$b."))";
				
	}

	if (($techpack_name =~ m/(DC_\w*)/) or ($techpack_name =~ m/(DIM_\w*)/)) {

		$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
		$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr>";
								
		$result_no.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
		my $query_from_etlrep;
		my @result_from_etlrep;
		my $query_from_etlrep1;
		my @result_from_etlrep1;
		my $query_from_etlrep2;
		my @result_from_etlrep2;
		my $modification;
		foreach my $key (keys %$data){
			$modification=$key;
		}	
		
		foreach my $modification_data (@{$data->{$modification}}) {
			#print "$modification_data->{Table}:$modification_data->{Type}:$modification_data->{OrderNo}:$modification_data->{ParentName}++";
		
			my $modify; 
			my $identity = $modification_data->{Identity};
				
			my @names = keys %$modification_data;
			if ( grep( /^Modify$/, @names ) and ($modification_data->{Type} eq "New")) {
				$modify = $modification_data->{Modify};
					my $str= "where ";
					my $str1;
					my $str2 = "collection_id = (select collection_id from meta_collections where collection_name like '$modification_data->{ParentName}' and version_number like '$version');";
					my $flag = 0;
					foreach my $key (keys %$modify){
						my $final_string="";
						if(index($modify->{$key}, "'") != -1){
							$str1="";
							$flag=1;

							$final_string=$modify->{$key};
							#replacing single quotes with double quote
							$final_string =~ s/'/"/g;
							
							$str1 ="$key = '".$final_string."' and ";
						}else{
						
							$str .= "$key = '".$modify->{$key}."' and ";
						}
					}
					my $query;
					if ($flag == 1){
						$query = "select * from $modification_data->{Table} ".$str.$str1.$str2;
					}else{
						$query = "select * from $modification_data->{Table} ".$str.$str2;
					}
				my $query_from_etlrep = "$query";
				#print "query_n :$query\n";
				@result_from_etlrep = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query_from_etlrep,"ROW");
		
				if(!@result_from_etlrep){
					#print "Fail\n";
					$fail_result++;
					$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$modification_data->{Table}</b></font></font></td></tr>";
					$result .= "<tr><td align=left><font color=800000><font size=2px><b>TRANSFER_ACTION_NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>ORDER NO</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
					$result .= "<tr><td align=left><font color=006633><b>$modify->{TRANSFER_ACTION_NAME}</b></font></td><td align=left><font color=006633><b>$modification_data->{OrderNo}</b></font></td><td align=left><font color=red><b>FAIL</b></font></td></tr>";
				}else{
					#print "Passed\n";
					$pass_result++;
					$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$modification_data->{Table}</b></font></font></td></tr>";
					$result .= "<tr><td align=left><font color=800000><font size=2px><b>TRANSFER_ACTION_NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>ORDER NO</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
					$result .= "<tr><td align=left><font color=006633><b>$modify->{TRANSFER_ACTION_NAME}</b></font></td><td align=left><font color=006633><b>$modification_data->{OrderNo}</b></font></td><td align=left><font color=006633><b>PASS</b></font></td></tr>";
				}
			}
						
			if ((($modification_data->{Type} eq "Update") or ($modification_data->{Type} eq "Delete")) ) {
			
				if(($modification_data->{Type} eq "Update")){
					my $mod_key;
					my $str1;
					my $modify1 = $modification_data->{Modify};
					my $str= "where ";
					my $str3;
					my $flag = 0;
					foreach my $key (keys %$modify1){
						$mod_key = "$key";
						$str1 .= "$key".",";
					}
					chop($str1);
					my $str2 = "collection_id = (select collection_id from meta_collections where collection_name like '$modification_data->{ParentName}' and version_number like '$version')";
					foreach my $key (keys %$identity){
						
						my @key_names = keys %$identity;
						if(grep( /^$mod_key$/, @key_names )){
							if($mod_key ne "$key"){
								
								my $final_string="";
								if(index($identity->{$key}, "'") != -1){
									$str3="";
									$flag=1;
									$final_string=$identity->{$key};
									#replacing single quotes with double quote
									$final_string =~ s/'/"/g;
									
									$str3 ="$key = '".$final_string."' and ";
								}else{
								
									$str .= "$key = '".$identity->{$key}."' and ";
								}
							}
						}else{
							
								$str .= "$key = '".$identity->{$key}."' and ";
						}
					}
			
					my $query1;
					if ($flag == 1){
						$query1 ="SELECT COUNT(*) FROM (select ".$str1." from $modification_data->{Table} ".$str.$str2.$str3.") AS subquery";
					}else{
						$query1 ="SELECT COUNT(*) FROM (select ".$str1." from $modification_data->{Table} ".$str.$str2.") AS subquery";
					}
					#print "dynamic query_u :$query1\n";
					
					$query_from_etlrep2="$query1";
					@result_from_etlrep2 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query_from_etlrep2,"ROW");
					
					if(!@result_from_etlrep2){
						#print "Fail\n";
						$fail_result++; 
						$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$modification_data->{Table}</b></font></font></td></tr>";
						$result .= "<tr><td align=left><font color=800000><font size=2px><b>TRANSFER_ACTION_NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>ORDER NO</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
						$result .= "<tr><td align=left><font color=006633><b>$identity->{TRANSFER_ACTION_NAME} $modify1->{TRANSFER_ACTION_NAME} </b></font></td><td align=left><font color=006633><b>$modification_data->{OrderNo}</b></font></td><td align=left><font color=red><b>FAIL</b></font></td></tr>";
					}else{
					
						if($result_from_etlrep2[0] == 1){
							#print "pass\n";
							$pass_result++;
							$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$modification_data->{Table}</b></font></font></td></tr>";
							$result .= "<tr><td align=left><font color=800000><font size=2px><b>TRANSFER_ACTION_NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>ORDER NO</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
							$result .= "<tr><td align=left><font color=006633><b>$identity->{TRANSFER_ACTION_NAME} $modify1->{TRANSFER_ACTION_NAME}</b></font></td><td align=left><font color=006633><b>$modification_data->{OrderNo}</b></font></td><td align=left><font color=006633><b>PASS</b></font></td></tr>";
						}else{
							$fail_result++; 
							$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$modification_data->{Table}</b></font></font></td></tr>";
							$result .= "<tr><td align=left><font color=800000><font size=2px><b>TRANSFER_ACTION_NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>ORDER NO</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
							$result .= "<tr><td align=left><font color=006633><b>$identity->{TRANSFER_ACTION_NAME} $modify1->{TRANSFER_ACTION_NAME} </b></font></td><td align=left><font color=006633><b>$modification_data->{OrderNo}</b></font></td><td align=left><font color=red><b>FAIL</b></font></td></tr>";
						}	
					}
				}
				if(($modification_data->{Type} eq "Delete")){
					my $modify1 = $modification_data->{Modify};
					my $flag = 0;
					my $str= "where ";
					my $str1; my $str3;
					my $str2 = "collection_id = (select collection_id from meta_collections where collection_name like '$modification_data->{ParentName}' and version_number like '$version');";
					foreach my $key (keys %$identity){
						my $final_string="";
						if(index($identity->{$key}, "'") != -1){
							$str1="";
							$flag=1;
							$final_string=$identity->{$key};
							#replacing single quotes with double quote
							$final_string =~ s/'/"/g;
							
							$str1 ="$key = '".$final_string."' and ";
						}else{
							$str .= "$key = '".$identity->{$key}."' and ";
						}
					}
					foreach my $key (keys %$modify1){
						my $final_string="";
						if(index($modify1->{$key}, "'") != -1){
							$str3="";
							$flag=1;
							$final_string=$modify1->{$key};
							#replacing single quotes with double quote
							$final_string =~ s/'/"/g;
							
							$str3 ="$key = '".$final_string."' and ";
						}else{
							$str .= "$key = '".$modify1->{$key}."' and ";
						}
					}
					my $query2;
					if ($flag == 1){
						$query2 = "select * from $modification_data->{Table} ".$str.$str1.$str3.$str2;
					}else{
						$query2 = "select * from $modification_data->{Table} ".$str.$str2;
					}
					#print "dynamic query2 :$query2\n";
				
					$query_from_etlrep1 ="$query2";
			
					@result_from_etlrep1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query_from_etlrep1,"ROW");
				
					if(!@result_from_etlrep1){

						$pass_result++;
						$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$modification_data->{Table}</b></font></font></td></tr>";
						$result .= "<tr><td align=left><font color=800000><font size=2px><b>TRANSFER_ACTION_NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>ORDER NO</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
						$result .= "<tr><td align=left><font color=006633><b>$identity->{TRANSFER_ACTION_NAME}</b></font></td><td align=left><font color=006633><b>$modification_data->{OrderNo}</b></font></td><td align=left><font color=006633><b>PASS</b></font></td></tr>";
					}else{
						$fail_result++;
						$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$modification_data->{Table}</b></font></font></td></tr>";
						$result .= "<tr><td align=left><font color=800000><font size=2px><b>TRANSFER_ACTION_NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>ORDER NO</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
						$result .= "<tr><td align=left><font color=006633><b>$modify->{TRANSFER_ACTION_NAME}</b></font></td><td align=left><font color=006633><b>$modification_data->{OrderNo}</b></font></td><td align=left><font color=red><b>FAIL</b></font></td></tr>";
					}
				}
			}

		}
	}	
	$result.=$result_no;
	return $result;
}

##################################################################################################################################

if($verifying_manmods eq "true")
   {
     print "********Executing Man mods verification Script********\n";

	 
	my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> VERIFY MAN_MODS  </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my $result1=verifying_manmods();
	    	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($pass_result) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($fail_result)</td>";
	 print "ManMods_Check:PASS- $pass_result FAIL- $fail_result \n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>$result1\t</td>";
	 $report.= "</table>";
	 
     $report.= getHtmlTail();
     my $file = writeHtml("VERIFY_MAN_MODS_".$datenew,$report);
     $verifying_manmods ="false";
	 
	 print "********Man_Mods Verification Checking Script Test Case is done********\n";
}
############################################################################################################################
