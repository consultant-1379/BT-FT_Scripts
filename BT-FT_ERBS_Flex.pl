#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';
use DBI;
use Data::Dumper;

##########################################################################################
# New script to verify the ERBS flex counters jira no. EQEV-120016 ##################### #
##########################################################################################

my $ERBS_Flex_check = "true";
my $result;
my $result_no;
my $num;
my $num1;
my $flag =0;

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



################################################################################################################
																							

############################################################
# Dataloading MO this will verify whether the data is matching with the EPFG input file.
# 
sub Verify_ERBS_Flex{

	my %hash_for_mapping_table_counter;
	my %hash_for_mapping_flex_counter;
	
	my %hash_for_mapping_measurement_counter;
	my %hash_for_mapping_table_counter1;
	my $Counter_data_measurementCounter;
	my $Key_measuerementcounter;
	my $table_name;
	my $counters;
	my $len;
	my $dir = $_[0];
	$result = '';
	my $fnum = 0;
	$flag = 0;

	
	

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

	my ($year,$hyphen1,$month,$hyphen2,$date,$space,$hour,$colon1,$min,$colon2,$sec) = $datetime =~ 
	m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;
	my $datetimeepfg;
	if(int($hour-2) < 10){
		$datetimeepfg="$year$month$date.0".(int($hour-2))."$min";}
		else{
		$datetimeepfg="$year$month$date.".(int($hour-2))."$min";}
	#print "EPFG time : $datetimeepfg\n";
	
	my $erbsg2datetime;
	if(int($hour-2) < 10){
		$erbsg2datetime="$year-$month-$date 0".(int($hour-2)).":$min:$sec";}
		else{
		$erbsg2datetime="$year-$month-$date " .(int($hour-2)).":$min:$sec";}
	#print "ErbsG2DateTime: $erbsg2datetime\n";
	
	my $package = $tp_name.":((".$build."))";
	
	if ($tp_name =~ m/(DC_\w*)/){

		$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
		$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr>";


		my $query_from_repdb = "select distinct substr(substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3),1,charindex(':',substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3))-1) as tab, DATANAME as counter from MeasurementColumn where  MTABLEID like '".$package."%_FLEX%:RAW' and COLTYPE like 'COUNTER' order by tab;";
		my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_repdb,"ALL");

		for my $data_from_repdb (@$result_from_repdb) {

			my ($table_name, $counter_name) = @$data_from_repdb;
			my $new_counter = "cast(".$counter_name." as Numeric(20,0))";
			push @{$hash_for_mapping_table_counter{$table_name}}, $counter_name;
			push @{$hash_for_mapping_table_counter1{$table_name}}, $new_counter;
			#print "$table_name,$counter_name";
		}
		
		my $csvfile_from_epfg;
		
		if($dir eq "RadioNode/LRAT"){
			$Nodename = "ERBSG201";
		}
		my $in_path = "/eniq/home/dcuser/epfg/txt/$dir";
		if(opendir ( DIR, $in_path )){
			$flag =1;
			my $intf_file = readdir(DIR);
			$csvfile_from_epfg = "/eniq/home/dcuser/epfg/txt/$dir/CountersTxt_".$datetimeepfg."_".$Nodename.".csv";
	
			my $temptag = '';
			open (EPFG_FILE , '<', $csvfile_from_epfg ) or die "ERROR: Couldn't open file for writing '$csvfile_from_epfg' $! \n";
			while (<EPFG_FILE>) {
				chomp;	

				my @epfg_counter_and_data = split(/;/, $_);

				my $counter_data_and_values = @epfg_counter_and_data;
				my $flexmatcher = "pmFlex";

				#print "$flexmatcher";
				if(grep(/^$flexmatcher/,@epfg_counter_and_data)){
	
					my ($tag,$tagid) = split(":",$epfg_counter_and_data[0]);
					my ($mo,$moid) = $epfg_counter_and_data[1] =~ m/(MOID:)(.*)/;
					
					my $tempcheck;
					my $erbstagid = $tagid;
					if($dir eq "RadioNode/LRAT"){
						($tempcheck,$erbstagid) = split("PmFlex",$tagid);
						
					}
					
					#print "$erbstagid tagids\n";
					#my ($tag,$tagid) = split(":",$epfg_counter_and_data[0]);
					my $i;
					#my ($mo,$moid) = $epfg_counter_and_data[1] =~ m/(MOID:)(.*)/;
					my $count = @epfg_counter_and_data;
					#print "CSV count : $count\n";

					for($i=2;$i<$count;$i++){
						my ($counter_flex,$value) = split(":",$epfg_counter_and_data[$i]);
						if($counter_flex =~ m/(pmFlex\w*)/){
			
							$hash_for_mapping_flex_counter{$counter_flex} = $value;
							#print "$counter_flex       $value\n";
						}
					}
					#my $query ;
			
					my $query = "select substr(substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3),1,charindex(':',substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3))-1) from defaulttags where tagid like '$erbstagid"."_FLEX%' and tagid not like '%DYN%' and dataformatid like '$package%'";
					$result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
					#print Dumper(\%hash_for_mapping_flex_counter);
					for my $tablename_repdb (@$result_from_repdb){

						#print "@$tablename_repdb[0]\n";

						if($temptag ne $tagid){
						$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
						$result .= "<tr><td align=left><font color=black><font size=4px><b>Table Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>@$tablename_repdb[0]</font></font></td></tr>";
						$result.= "<tr><td align=left><font color=blue><font size=3px><b>COUNTER NAME</b></font></font></td><td align=left><font color=blue><font size=3px><b>FLEX_FILTER</b></font></font></td><td align=left><font color=blue><font size=3px><b>MOID</b></font></font></td><td align=left><font color=blue><font size=3px><b>RESULT</b></font></font></td></tr>";

						}


						$counters = join(',',@{$hash_for_mapping_table_counter1{@$tablename_repdb[0]}});
						$len =@{$hash_for_mapping_table_counter{@$tablename_repdb[0]}};

						#print "$len\n";
						if($dir eq "RadioNode/LRAT"){
							$datetime = $erbsg2datetime;
						}

						$query = "select $counters,flex_filtername from @$tablename_repdb[0]_raw where DATETIME_ID = '$datetime' and $Nodenamekey like '$Nodename' and moid like '$moid'";
				
						#print "$query\n";
						my $result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
						#print Dumper(\@$result_from_dwhdb);
						$fnum = 0;
						for my $i (@$result_from_dwhdb){
							my @temp = @{$hash_for_mapping_table_counter{@$tablename_repdb[0]}};
							for(my $j=0;$j<$len;$j++){
						
								if(@$i[$len] eq ""){

									#print "$hash_for_mapping_flex_counter{$temp[$j]} eq @$i[$j]\n";
										if(grep(/^$temp[$j]$/,keys %hash_for_mapping_flex_counter)){
											#print "$temp[$j]\n";
											if($hash_for_mapping_flex_counter{$temp[$j]} eq @$i[$j]){
												#print "$temp[$j]    pass\n";
												#$result .= "<tr><td align=left><font color=black><b>$temp[$j]</b></font></td><td 	align=left><font color=black><b></b></font></td><td align=left><font color=black><b>$moid</b></font></td><td align=left><font color=FF0000><b>PASS</b></font></td></tr>";
												$num++;
											}else{
												#print "$temp[$j] fail\n";
												$result .= "<tr><td align=left><font color=black><b>$temp[$j]</b></font></td><td 	align=left><font color=black><b></b></font></td><td align=left><font color=black><b>$moid</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
												$num1++;
												$fnum++;
											}
										}
							
								}else{
									my $t = "$temp[$j]_@$i[$len]";
									#print "****$hash_for_mapping_flex_counter{$t} eq @$i[$j]\n";
									if(grep(/^$t$/,keys %hash_for_mapping_flex_counter)){
										if($hash_for_mapping_flex_counter{$t} eq @$i[$j]){
											#print "$t     tpass\n";

											#$result .= "<tr><td align=left><font color=black><b>$temp[$j]</b></font></td><td align=left><font color=black><b>@$i[$len]</b></font></td><td align=left><font color=black><b>$moid</b></font></td><td align=left><font color=FF0000><b>PASS</b></font></td></tr>";

											$num++;
										}else{							
											#print "$t tfail\n";

											$result .= "<tr><td align=left><font color=black><b>$temp[$j]</b></font></td><td align=left><font color=black><b>@$i[$len]</b></font></td><td align=left><font color=black><b>$moid</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";

											$num1++;
											$fnum++;
										}
								
									}
								}
								
						
							}
						}
						if($fnum eq 0){
							$result .= "<tr><td align=left><font color=008000><b>All Counters are passed</b></font></td><td align=left><font color=008000><b> </b></font></td><td align=left><font color=008000><b>$moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";}

					}
				

					$temptag=$tagid;
				}
			}
		}	
	}
				
				
			

		$result.= $result_no;	
		return $result;

}
##############################################################################################################################################

if($ERBS_Flex_check eq "true")
   {
     print "********Executing Verification of ERBS_Flex Check Script********\n";
	
	my @files = ("lterbs","RadioNode/LRAT");
	for my $dir (@files){
	#print "@files\n";
	$num=0;
	$num1=0;
	my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> VALIDATION OF ERBS FLEX COUNTERS </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my $result1 = Verify_ERBS_Flex($dir);
	 #print "$num\n$num1\n";
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($num) / <font size = 2 color=red><b>FAIL ($num1)</td>";
	 
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.="$result1";
	 
     $report.= getHtmlTail();
	 if($dir eq "lterbs"){
	 
		$dir = "ERBS";
	}else{
		$dir = "ERBSG2";
		}
	if($num eq 0 and $num1 eq 0 and $flag eq 0) {
		#print "$dir"."_Flex_Counter_Validation:PASS- $num FAIL- $num1\n";
		my $file = writeHtml("VERIFY_TABLE_$dir"."_FLEX".$datenew,$report);
	}else{
		print "$dir"."_Flex_Counter_Validation:PASS- $num FAIL- $num1\n";
		my $file = writeHtml("VERIFY_TABLE_$dir"."_FLEX".$datenew,$report);
	
	}
	
	 
	 #print "PARTIAL FILE: $file\n"; 
     #$dataloadingmo ="false";
	 }
	 print "********Verification of ERBS_Flex Check  Script Test Case is done********\n";
   }


