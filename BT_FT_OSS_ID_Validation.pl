#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $keysloading = "true";
my $result;

my $num = 0;
my $num1 = 0;
my $empty_num = 0;
my $result_no;

my $oss_directory_check = "true";
my $result1;
my $fail_result = 0;
my $pass_result = 0;
my $result_no1;
my $empty_num1 = 0;
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
		 elsif ( $type eq "HASH" ) {
                $sel_stmt->execute() or warn $DBI::errstr;
                my $result = $sel_stmt->fetchrow_hashref();
                $sel_stmt->finish();
                return $result;
        }
        $dbh->disconnect;
}


																							
sub uniq {
			return keys  %{{ map { $_ => 1 } @_ }};
	}
##################################################################################################################################

sub keysloading{

				my @array;
				my $i;
				my $line;
				my %hash;
				my %hash1;
				my %hash_;
				my %hash1_;
				my $table_nam;
				my @result;
				my $report="";
				
				chdir "/eniq/home/dcuser/BT-FT_Script" or die $!;
					
				my $time_file = 'time.txt';
				open TIME, '<', $time_file or die "1.Could not open '$time_file', No such file found in the provided path $!\n";
				my $today = <TIME>;
				#print "$today\n";
				

				
				my $file = 'data.txt';
				open FILE, '<', $file or die "2.Could not open '$file', No such file found in the provided path $!\n";
				@array = <FILE>;
				#print "@array\n";
				my $count = @array;
				#print "$count\n";
				
				for ($i=0;$i<$count;$i++){
				
				undef $line;
	            $line = shift @array;
				chomp $line;
				my ($strings,$r,$b) = $line  =~ m/(\w*_\w*)(\_R.+\_b)(\d+)/;
				
				my $package = $strings.":((".$b."))";
				
				if ($strings=~ m/(INTF_\w*)/){
				
				print "This testcase is not applicable for Interfaces.";				
				
				}
				else{
				foreach my $tp_names ($package){
				
				$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp_names" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
				$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left><font color=blue><font size=5px><b>$tp_names</b></font></font></td></tr>";
				$report.= "</table>";
				$report.= "<br>";
				
				$result_no.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp_names" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
			#no data
				$result_no.= "<tr><td align=left><font size=3px><font color=magenta><b>NO DATA IN TABLE FOR DATE $today:</b></font></font></td></tr>";
				
				
				#print "The TechPack/Node name is $tp_names \n\n";
				my $query = "select distinct substr(substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3),1,charindex(':',substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3))-1) as tab, DATANAME as counter from MeasurementColumn where  MTABLEID like '".$tp_names."%:RAW' and COLTYPE like '%KEY' order by tab;";
				my $res = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
				
				undef %hash;
				undef %hash1;
				undef %hash_;
				undef %hash1_;
#______________________________________________________________________________________________________________________________________________________________________
				for my $data (@$res){
				
				my ($table, $counter) = @$data;
				push @{$hash{$table}}, $counter;
										
				}
				#### While loop to remove the duplicates column names in HASH1
                while ( my ( $k , $v) = each (%hash)){
                                                my @uniq_value;
                                                my @temp_array = @$v;
                                                my %temp_hash = map { $_, 0 } @temp_array;
                                                @uniq_value = keys %temp_hash;
                                                push @{$hash_{$k}} , @uniq_value;
                  } # While Loop Closed for uniq array in HASH1
				  #print Dumper(\%hash);
				
				foreach my $key ( keys %hash ){
				undef $table_nam;
				$table_nam = $key."_RAW";
				chomp($table_nam);
				
					
				$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left><font color=blue><font size=3px><b>$table_nam</b></font></font></td></tr>";
				$result .= "<tr><td align=left><font color=800000><font size=2px><b>KEY NAME</b></font></font></td><td align=left><font color=800000><b>VALUE</b></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
	
                    my $datetimeid_10 = $today." 10:00:00";
					my $datetimeid_11 = $today." 11:00:00";
					
					
					#Added to verify oss
					my $query3 = "select distinct OSS_ID from $table_nam" ;
					my @res3 = executeSQL("dwhdb",2640,"dc",$DCPass,$query3,"ALL");
					
					my @oss=undef;
					foreach my $i(@res3){
					
						foreach my $j(@$i){
					
							foreach my $k(@$j){
								
								push(@oss , $k);
									
							}
						}
					}
					
					if ( !@res3 )
								{
					#print "No data in table\n";
					$empty_num++;
					
					$result_no.= "<tr><td align=left><font color=000000><b>$table_nam</b></font></td></tr>";
					}
					
					else
					{
					my @output2 = undef;
                        push (@output2 ,@oss);
						
						my @array11 = split (",","OSS_ID");
						
						my $count1 = @array11;

						my $a;
						for ($a=0;$a<1;$a++){
						
						my @array22 = split (";","@output2");
						
						my $count2 = @array22;
						my $b;
						
						for ($b=0;$b<$count2;$b++){
						
						my $final_result;
						
						if((grep(/^eniq_oss_1$/, @oss)) && (grep(/^eniq_oss_2$/, @oss))){
						
						$final_result = 'PASS';
						
						}
						else {
						
						$final_result = 'FAIL';
						}
						if ($final_result eq 'PASS'){
						$num1++;
						$result .= "<tr><td align=left><font color=000000><b>$array11[$b]</b></font></td><td align=left><font color=000000><b>$array22[$b]</b></font></td><td align=left><font color=008000><b>$final_result</b></font></td></tr>";
						}
						else {
						$num++;
						$result .= "<tr><td align=left><font color=000000><b>$array11[$b]</b></font></td><td align=left><font color=000000><b>$array22[$b]</b></font></td><td align=left><font color=FF0000><b>$final_result</b></font></td></tr>";
						}
						
						$report.= "</table>";
						$report.= "<br>";
						
						}
						
						}
						
					}			
				}
				}				
				}
				}
			
$result.=$result_no;
return $result;

}
############################################################################################################################

   
sub oss_directory_check {

my @final_table_name = ();
my $flag=undef;
my $flag1=undef;

my $file = 'data_new.txt';
open FILE, '<', $file or die "3.Could not open '$file', No such file found in the provided path $!\n";
my @array = <FILE>; 
my $line = ();
$line = shift @array;
chomp $line;
#print "$line\n";
my $f;
	
my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)/;

my ($datetime_year,$hyphen1,$datetime_month,$hyphen2,$datetime_date,$space,$datetime_hour,$colon1,$datetime_min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;

my $date_for_filename = $datetime_year.$datetime_month.$datetime_date;
my $time_for_filename = $datetime_hour.$datetime_min;
 
my $package = $tp_name.":((".$build."))";

if ($tp_name =~ m/(DC_\w*)/) {

	$result1.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result1 .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr>";
	
	
	$result_no1.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	#for no data				
	#$result_no1.= "<tr><td align=left><font size=3px><font color=magenta><b>NO DATA IN TABLE FOR DATE $datetime AND NODE-NAME: $Nodename</b></font></font></td></tr>";

	my $query_from_repdb = "select distinct TYPENAME from MeasurementType where TYPEID like '%".$package."%' and TYPENAME not like '%BH' order by TYPENAME";
	#print "$query_from_repdb \n";
	my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_repdb,"ALL");
	#print "$result_from_repdb \n";
	
	for my $rows ( @$result_from_repdb ) {
						for my $field ( @$rows ) {
								if ( $field !~ m/^dim*/ || $field !~ m/^DIM*/ )
							{
								push @final_table_name, $field;
								#print " fi: $field \n";
							}
						}
					}
	
	
	my $query_for_INTF = "select INTERFACENAME from  InterfaceTechpacks where TECHPACKNAME LIKE '".$tp_name."'";
	$result1.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result1 .= "<tr><td align=left><font color=orange><font size=2px><b>ENIQ_OSS_1:</b></font></font></tr>";
	$result1 .= "<tr><td align=left><font color=blue><font size=2px><b>Interface Name:</b></font></font></td><td align=left><font color=maroon><font size=2px><b>RESULT</b></font></font></td></tr>";	
	
	
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
		
	
	#chdir $in_path or die $!;
	my $correct_intf_file;
	$flag=0;
		
	if(opendir ( DIR, $in_path )){
	while( my $intf_file = readdir(DIR)) {
		
		my ($correct_intf_file) = $intf_file =~ m/(INTF_\w*\_$date_for_filename\.$time_for_filename\.txt)/;
		 
		if ($intf_file eq $correct_intf_file) {
			
			$flag=1;
			}
			}
				closedir(DIR); 
			}
		if($flag==1){
		
		$pass_result++;
		$result1 .= "<tr><td align=left><font color=black><font size=2px><b>$f</b></font></font></td><td align=left><font color=green><font size=2px><b>PASS</b></font></font></td></tr>";	
	
		}
		
		else{ 
		
		$fail_result++;
		$result1 .= "<tr><td align=left><font color=black><font size=2px><b>$f</b></font></font></td><td align=left><font color=red><font size=2px><b>FAIL</b></font></font></td></tr>";	
		}
		
		}
		}}
		}}
		}

#eniq_oss_2
				my $query = "select INTERFACENAME from  InterfaceTechpacks where TECHPACKNAME LIKE '".$tp_name."'";
					$result1.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					$result1 .= "<tr><td align=left><font color=orange><font size=2px><b>ENIQ_OSS_2:</b></font></font></td></tr>";
					$result1 .= "<tr><td align=left><font color=blue><font size=2px><b>Interface Name:</b></font></font></td><td align=left><font color=maroon><font size=2px><b>RESULT</b></font></font></td></tr>";	
	
				$result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
				for my $r ( @$result_from_repdb ) {
				  for my $f ( @$r ) {
					$query = "select COLLECTION_SET_ID from META_COLLECTION_SETS WHERE COLLECTION_SET_NAME LIKE '".$f."-eniq_oss_2'";
					my @result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
					#print $result_from_repdb1[0];
					$query = "select substr(action_contents_01,charindex('processedDir',action_contents_01)+39,charindex('/processed',action_contents_01)-(charindex('processedDir',action_contents_01)+39)) procdir from meta_transfer_actions where collection_set_id = '$result_from_repdb1[0]'and action_type like 'parse'";
					@result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
					my $in_path1="/eniq/archive/eniq_oss_2/".$result_from_repdb1[0]."/processed";
					#print "final path is $in_path1\n";
					#chdir $in_path1 or die $!;
					
					$flag1=0;
						
		if(opendir ( DIR, $in_path1 )){
		
			while( my $intf_file1 = readdir(DIR)) {
	
		my ($correct_intf_file1) = $intf_file1 =~ m/(INTF_\w*\_$date_for_filename\.$time_for_filename\.txt)/;
		
		#print "$intf_file1\n";
		#print "$correct_intf_file1\n";
		if ($intf_file1 eq $correct_intf_file1) {
		
		 $flag1=1;
		 }
		 }
		closedir(DIR);	 
		 }
		 
		if($flag1 == 1){
				
				$pass_result++;
				$result1 .= "<tr><td align=left><font color=black><font size=2px><b>$f</b></font></font></td><td align=left><font color=green><font size=2px><b>PASS</b></font></font></td></tr>";	

		}
		
		else{ 
		
				$fail_result++;
				$result1 .= "<tr><td align=left><font color=black><font size=2px><b>$f</b></font></font></td><td align=left><font color=red><font size=2px><b>FAIL</b></font></font></td></tr>";	
		
		}
		}}

$result1.=$result_no1;
return $result1;
	}
else {

	print "$tp_name: This is not a PM TP. This testcase is only for PM TP.\n\n\n";

	}
}
############################################################################################################################

     print "********Executing OSS_ID Checking Script********\n";

	  my $report =getHtmlHeader();
	
	#process
     $report.="<h1> <font color=MidnightBlue><center> <u>OSS_ID DIRECTORY CHECK </u> </font> </h1>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	 my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	$result1= oss_directory_check();
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($pass_result) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($fail_result)/ <a href=\"#t3\"><font size = 2 color=magenta><b>No Data in table ($empty_num)</td>";
	 #--print "Directory_Check:PASS- $pass_result FAIL- $fail_result No_Data- $empty_num\n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>$result1\t</td>";
	 $report.= "</table>";
	
	#OSS
	$report.="<h1> <font color=MidnightBlue><center> <u> OSS_ID DATABASE CHECK </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	$stime = getTime();
	$report.= "<td><b>$stime\t</td>";
	 
	$result1=keysloading();
	   	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
      $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($num1) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($num)/ <a href=\"#t3\"><font size = 2 color=magenta><b>No Data in table ($empty_num)</td>";
	 my $pass1=$num1+$pass_result;
	 my $fail1=$num+$fail_result;
	 print "OSS_ID_VALIDATION:PASS- $pass1 FAIL- $fail1 No_Data- $empty_num\n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>$result1\t</td>";
	 $report.= "</table>";
	 
     $report.= getHtmlTail();
     my $file = writeHtml("OSS_ID_VALIDATION_".$datenew,$report);
	 #print "PARTIAL FILE: $file\n"; 
     #$keysloading ="false";
	 print "********OSS_ID VALIDATION Script Test Case is done********\n";
   
   

############################################################################################################################