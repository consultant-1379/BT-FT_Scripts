#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';
use DBI;
use Data::Dumper;
use Text::CSV;


my $fileStamp = $ARGV[0];
my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
my $RESULTPATH="/eniq/home/dcuser/ResultFiles";

my $pass = 0;
my $fail = 0;
my $today1;
my %perTPRes;

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
###################################################################################

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
	return qq{<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<title>
Aggregation Status
</title>
<STYLE TYPE="text/css">
</STYLE>
</head>
<body>
};
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
	my $testcase = shift;
	my $out    = shift;
	my $filename = "$LOGPATH/$testcase.html";
	open(my $fhandle, ">", $filename) or die "Couldn't open: $!";
	print $fhandle $out;
	close $fhandle;
	return $filename;
}

############################################################
# WRITE Result
# This is a utility for result file for summary page

sub writeResult{
	my $filename = "$RESULTPATH/" . $_[0].".txt";
	open(my $fhandle, ">", $filename) or die "Couldn't open: $!";
	foreach my $tp (keys %perTPRes) {
		print "$tp=$perTPRes{$tp}\n";
		print $fhandle "$tp=$perTPRes{$tp}\n";
	}
	close $fhandle;
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
	#print "$arg\n";
	my $connstr = "ENG=$dbname;CommLinks=tcpip{host=localhost;port=$port};UID=$cre;PWD=$cre1";
	my $dbh = DBI->connect( "DBI:SQLAnywhere:$connstr", '', '', {AutoCommit => 1} ) or warn $DBI::errstr;
    my $sel_stmt=$dbh->prepare($arg) or warn $DBI::errstr;
	if ( $type eq "ROW" ) {
		$sel_stmt->execute() or warn $DBI::errstr;
        my @result = $sel_stmt->fetchrow_array();
        $sel_stmt->finish();
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
#Enter descriptions for all
#

sub check_aggStatus{
	my $type_id = $_[0];
	my $status_query = "SELECT STATUS FROM LOG_AggregationStatus WHERE TIMELEVEL='DAY' AND TYPENAME='$type_id' AND DATADATE='$today1'";
	#print "status Query :$status_query\n";
	my @status = executeSQL("dwhdb",2640,"dc",$DCPass,$status_query,"ROW");
	return $status[0];
}

sub getAggTablesFromTP{
	my $techPack = $_[0];
	my $dayAgg_query = "SELECT A.TYPENAME, A.TYPEID, A.DELTACALCSUPPORT FROM MeasurementType AS A, TPActivation AS B WHERE B.VERSIONID = A.VERSIONID AND B.TECHPACK_NAME = '$techPack' AND A.TOTALAGG = 1 AND A.RANKINGTABLE = 0";
	my $dayAgg = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$dayAgg_query,"ALL");
	return $dayAgg;
}

#sub checkIfPM{
#	my $pack = $_[0];
#	my $pm_query = "SELECT TYPE FROM TPActivation WHERE TECHPACK_NAME='$pack'";
#	my @type = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$pm_query,"ROW");
#	if (@type){
##			return 1;
#	}
#	}
#	return 0;
#}

sub getRawData{
	my $keys = $_[0];
	my $counters = $_[1];
	my $rawTable = $_[2];
	my $countAgg = $_[3];
	my $Nnk = $_[4];
	my $Nodenames = $_[5];
	
	if ($countAgg eq "0"){
		$rawTable .= "_RAW";
	}
	else{
		$rawTable .= "_COUNT";
	}
	my $result;
	my $Nnk1=$Nnk;
	my @keysList = ();
	my @dupConList = ();
	my @counterList = ();
	foreach my $key ( @$keys ){
		my ($keyName,$unique) = @$key;
		if ($unique eq "0"){
			push @keysList, "MIN(".$keyName.")";
			if($keyName eq $Nnk){
				$Nnk1 = "MIN($keyName)";
			}
		}
		else{
			push @keysList, $keyName;
			push @dupConList, $keyName;
		}
	}
	foreach my $counter ( @$counters ){
		my ($counterName,$timeAgg) = @$counter;
		if ($timeAgg ne 'NONE'){
		push @counterList, "$timeAgg($counterName)";}
	}
	my $columns = join(",",(@keysList,@counterList));
	my $dupKeys = join(",",(@dupConList));
	my $getRawData_query = "select $Nnk1,$columns from $rawTable where ROWSTATUS not in ('DUPLICATE','SUSPECTED') and DATE_ID = '$today1' and $Nnk in ('$Nodenames') and oss_id like 'eniq_oss_1' group by $dupKeys order by $dupKeys";
	#print "Query Raw :$getRawData_query\n";
	$result = executeSQL("dwhdb",2640,"dc",$DCPass,$getRawData_query,"ALL");
	return $result;
}

sub getDayData{
	my $keys = $_[0];
	my $counters = $_[1];
	my $dayTable = $_[2]."_DAY";
	my $Nnk = $_[3];
	my $Nodename1 = $_[4];
	my $result;
	my @keysList = ();
	my @dupConList = ();
	my @counterList = ();
	my @nonelist=();
	my $Nnk1=$Nnk;
	
	foreach my $key ( @$keys ){
		my ($keyName,$unique) = @$key;
		if ($unique eq "0"){
			push @keysList, "MIN($keyName)";
			if($keyName eq $Nnk){
				$Nnk1 = "MIN($keyName)";
			}
		}
		else{
			push @keysList, $keyName;
			push @dupConList, $keyName;
		}
	}
	foreach my $counter ( @$counters ){
		my ($counterName,$timeAgg) = @$counter;
		if($timeAgg ne 'NONE'){
		push @counterList, "$timeAgg($counterName)";}
		else
		{
			push @nonelist,$counterName;
		}
		
	}
	my $columns = join(",",(@keysList,@counterList));
	my $dupKeys = join(",",(@dupConList));
	my $getDayData_query = "select $Nnk1,$columns from $dayTable where ROWSTATUS not in ('DUPLICATE','SUSPECTED') and DATE_ID = '$today1' and $Nnk in ('$Nodename1') and oss_id like 'eniq_oss_1' group by $dupKeys order by $dupKeys";
	#print "Query day :$getDayData_query\n";
	$result = executeSQL("dwhdb",2640,"dc",$DCPass,$getDayData_query,"ALL");
	my $len=@nonelist;
	my $result1=();
	if ($len > 0){
	$columns = join(",",(@nonelist));
	$getDayData_query = "select $Nnk,$columns from $dayTable where ROWSTATUS not in ('DUPLICATE','SUSPECTED') and DATE_ID = '$today1' and $Nnk in ('$Nodename1') and oss_id like 'eniq_oss_1'";
	$result1 = executeSQL("dwhdb",2640,"dc",$DCPass,$getDayData_query,"ALL");
	}
	my @re=();
	push @re,($result,$result1);
	return @re;
}
############################################################
# Enter valid description
# 

sub aggregation_status{
	my $result;
	my $typename = undef;
	my $typeid = undef;
	my $delta = undef;
	my @tblname;
	my $RAW_DATA = undef;
		
	my $file = 'data_new.txt';
	open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
	my @array = <FILE>;
	
	my $line = ();
	$line = shift @array;
	chomp $line;	
	my ($tp_name1,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNameKey) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.+)/;
	
	#EQEV-126991 : BT-FT script to be enhanced to handle multiple Node name.
	my @input_node_array= split(",",$Nodename);
	
	my $node_names = join("','",@input_node_array);
	#print Dumper(\@input_node_array);	
	
	my ($year,$hyphen1,$month,$hyphen2,$date,$space,$hour,$colon1,$min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;
	$today1 = "$year-$month-$date";
	
		my $tp_name = $tp_name1;
		my $tp_fail = 0;
		$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table id="$tp_name" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
		$result .= "<tr><th colspan=4 align=left><font color=black><font size=5px><b>TP NAME: $tp_name</b></font></font></th></tr>";
		$result .= "<tr><th align=left><font color=000000><b>TABLE NAME</b></font></th><th align=left><font color=000000><b>NodeNames</b></font></th><th align=left><font color=000000><b>RESULT</b></font></th><th align=left><font color=000000><b>STATUS</b></font></th></tr>";
		my $table_names = getAggTablesFromTP($tp_name);
		foreach my $tbl( @$table_names ){
			undef $typename;
			undef $typeid;
			undef $delta;
			($typename, $typeid, $delta) = @$tbl;
			#$result .= "<tr><td align=left><font color=0000FF><b>$typename</b></font></td>";
			#print "*******************************\ntypename :$typename\n*******************************************\n";
 			my $agg_status = check_aggStatus($typename);
			my @out_node_names_pass;
			my @out_node_names_fail;
			if ($agg_status eq "AGGREGATED"){
				#Validate data
				my $getKeys = "SELECT DATANAME,UNIQUEKEY FROM MeasurementKey WHERE TYPEID='$typeid'";
				my $getCounters = "SELECT DATANAME,TIMEAGGREGATION FROM MeasurementCounter WHERE TYPEID='$typeid'";
				my $measKeys = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$getKeys,"ALL");
				my $measCounters = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$getCounters,"ALL");
				#print "$typename\n";
				$RAW_DATA = getRawData($measKeys,$measCounters,$typename,$delta,$Nodenamekey,$node_names);
				my ($DAY_DATA, $NoneData) = getDayData($measKeys,$measCounters,$typename,$Nodenamekey,$node_names);
				if (@$RAW_DATA){
					if (@$DAY_DATA){
						my $agg_fail = 0;
						
						my @uniq_out_Node_values_pass;
						my @uniq_out_Node_values_fail;
						my $log_node_names_pass="";
						my $log_node_names_fail="";
						undef @out_node_names_pass;
						undef @out_node_names_fail;
						
						foreach my $index (0..$#{$RAW_DATA}){
							
							foreach my $inner_index (0..$#{$RAW_DATA->[$index]}){
							if ($RAW_DATA->[$index]->[$inner_index] =~ m/([-]?[\d]*\.[\d]{3})/) {
								$RAW_DATA->[$index]->[$inner_index] = $1;  
							}
							if ($DAY_DATA->[$index]->[$inner_index] =~ m/([-]?[\d]*\.[\d]{3})/) {
								$DAY_DATA->[$index]->[$inner_index] = $1;  
							}
							
							#print "RAW :$RAW_DATA->[$index]->[0]==DAY :$DAY_DATA->[$index]->[0]\n";
							
								if($RAW_DATA->[$index]->[$inner_index] eq $DAY_DATA->[$index]->[$inner_index]){
									push(@out_node_names_pass,$RAW_DATA->[$index]->[0]);
									
									#$pass++;
								}else{
									
									#$fail++;
									push(@out_node_names_fail,$RAW_DATA->[$index]->[0]);
									$agg_fail++;
									
								}
							}
						}
						#------------- removing duplicate values from the array  -------#										
							sub uniq {
								my %seen;
								grep !$seen{$_}++, @_;
							}
							@uniq_out_Node_values_pass = uniq(@out_node_names_pass);
							$log_node_names_pass = join(',', @uniq_out_Node_values_pass);
							
							@uniq_out_Node_values_fail = uniq(@out_node_names_fail);
							$log_node_names_fail = join(',', @uniq_out_Node_values_fail);							

						#------------- end removing duplicate values from the array  -------#
						my @out_node_names_fail_none;
						my @uniq_out_Node_values_fail_none;
						my $log_node_names_fail_none;
						
						foreach my $index (0..$#{$NoneData}){
							foreach my $inner_index (1..$#{$NoneData->[$index]}){
								if ($NoneData->[$index]->[$inner_index] ne ""){
									$agg_fail++;
									push(@out_node_names_fail_none,$RAW_DATA->[$index]->[0]);
									#print "Data mismatch :$RAW_DATA->[$index]->[0]\n";
								}
								#print "$NoneData->[$index]->[$inner_index]\n";
							}
						}
						# for none list check for logs - as part of EQEV-126991 jira
						@uniq_out_Node_values_fail_none = uniq(@out_node_names_fail_none);
						$log_node_names_fail_none = join(',', @uniq_out_Node_values_fail_none);
						
						if ($agg_fail == 0){
							#----- commenting the pass results writing on the logs to reduce the size -------#
							#$result .= "<td align=left><font color=green><b>$log_node_names_pass</b></font></td><td align=left><font color=green><b>PASS</b></font></td><td align=left><font color=green><b>$agg_status</b></font></td></tr>";
							$pass++;
						}else{
							$result .= "<tr><td align=left><font color=0000FF><b>$typename</b></font></td>";
							$result .= "<td align=left><font color=FF4500><b>$log_node_names_fail$log_node_names_fail_none</b></font></td><td align=left><font color=FF4500><b>FAIL</b></font></td><td align=left><font color=FF4500><b>AGGREGATED DATA MISMATCH</b></font></td></tr>";
							$fail++;
							$tp_fail++;
						}
					}else{
						$result .= "<tr><td align=left><font color=0000FF><b>$typename</b></font></td>";
						$result .= "<td align=left><font color=FF4500><b>$Nodename</b></font></td><td align=left><font color=FF4500><b>FAIL</b></font></td><td align=left><font color=FF4500><b>NO DATA IN DAY</b></font></td></tr>";
						$tp_fail++;
						$fail++;
					}
				}else{
					$result .= "<tr><td align=left><font color=0000FF><b>$typename</b></font></td>";
					$result .= "<td align=left><font color=FF4500><b>$Nodename</b></font></td><td align=left><font color=FF4500><b>FAIL</b></font></td><td align=left><font color=FF4500><b>NO DATA IN RAW</b></font></td></tr>";
					$tp_fail++;
					$fail++;
				}
			}else{
				$result .= "<tr><td align=left><font color=0000FF><b>$typename</b></font></td>";
				$result .= "<td align=left><font color=FF4500><b>$Nodename</b></font></td><td align=left><font color=FF4500><b>FAIL</b></font></td><td align=left><font color=FF4500><b>$agg_status</b></font></td></tr>";
				$tp_fail++;
				$fail++;
			}
			}
			$result.= "</table>";
			#if ($tp_fail == 0){
			#	$perTPRes{$tp_name} = "PASS";
			#}
			#else{
			#	$perTPRes{$tp_name} = "FAIL";
			#}
	#}
	return $result;
}
########################################################MAIN######################################################################################

print "********Executing Aggregation Status Script********\n";

my $report = getHtmlHeader();
$report .= "<h1> <font color=MidnightBlue><center> <u> AGGREGATION STATUS </u> </font> </h1>";
$report .= qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
$report .= "<tr>";
$report .= "<td><font size = 2 ><b>START TIME:\t</td>";
my $stime = getTime();
$report .= "<td><b>$stime\t</td>";
my $result1 = aggregation_status();
$report .= "<tr>";
$report .= "<td><font size = 2 ><b>END TIME:\t</td>";
my $etime = getTime();
$report .= "<td><b>$etime\t</td>";
$report .= "<tr><td><b>RESULT</td><td><b>Pass:$pass/Fail:$fail</td></tr>";
print "Aggregation_Status:PASS- $pass FAIL- $fail\n";
$report .= "</table>";
$report .= "<br>";
$report .= $result1;
$report .= getHtmlTail();
my $file = writeHtml("Aggregation_Status_".$datenew,$report);
#writeResult("Aggregation_Status");
#print "AGGREGATION STATUS OUTPUT: $file\n"; 
print "********Aggregation Status Script Test Case is done********\n";
