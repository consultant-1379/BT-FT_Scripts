#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $result;
my $report="";
my $result_no;
my $num = 0;
my $num1 = 0;
my $empty_num = 0;
my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNamekey);
my $package;
my $FNODE;
my @TopoNODE=();
my $flag;

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
return qq{<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<title>
Node Name Key Data Validation
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

sub TopoData
{
	####Get Topo Data
	my $intf = $_[1];
	my $DataFormat = $_[0];
	my @TopoTable=();
	my $query;
	if($intf eq "INTF_DC_E_SHARED_CNF")
	{
		push(@TopoTable,"DIM_E_SHARED_CNF_ALL");
		push(@TopoTable,"DIM_E_SHARED_CNF_NE");
	}
	else
	{
	$query="select distinct substr(config,charindex('basetable=',config)+10) as c from transformation where transformerid like '%$package%$DataFormat' and config like '%basetable%' and c not like '%SITE' and c not like 'DIM_TIMELEVEL'";
	my $result_from_repdb1= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	#print Dumper(\@result_from_repdb1);
	for my $r(@$result_from_repdb1)
	{
		push(@TopoTable,@$r[0]);
	}
	}
	for my $i (@TopoTable)
	{
		$query = "select distinct DATANAME from referencecolumn where typeid like '%$i%' and dataname like '$TopoNodeNamekey'";
		my $res = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
		my $len = @$res;
		if($len ne 0){
	$query="select $TopoNodeNamekey from $i where oss_id like 'eniq_oss_1'";
	#print "$query\n";
	my $result_from_dwhdb= executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
	for my $i (@$result_from_dwhdb)
	{
		push(@TopoNODE,@$i[0]);
	}
	}
}
}

sub Contains
{
	my $Hunt = $_[0];
	my @Treasure = @{$_[1]};
	#print Dumper(\@Treasure);
	for my $i (@Treasure)
	{
		if($i eq $Hunt)
		{return 1;}
	}
	return 0;
}


sub NodeNameValidation{

				my @array;
				my $i;
				my $line;
				my %hash;
				my $table_name;
				my @result;
				my $flag=0;
				
				my $file = 'data_new.txt';
				open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
				@array = <FILE>;
				my $Number = @array;
				#print "$Number\n";
				
				for ($i=0;$i<$Number;$i++)
				{
				my $line = ();
				$line = shift @array;
				chomp $line;	
				($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNamekey) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.+)/;
				@array = split(",",$Nodename);
				$Nodename = $array[0];
				
				my ($date,$time)=split(" ",$datetime);
				$package = $tp_name.":((".$build."))";
				
				my ($datetime_year,$hyphen1,$datetime_month,$hyphen2,$datetime_date,$space,$datetime_hour,$colon1,$datetime_min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;
				my $date_for_filename = $datetime_year.$datetime_month.$datetime_date;
				my $time_for_filename = $datetime_hour.$datetime_min;

				if($tp_name eq "DC_E_ERBS" or $tp_name eq "DC_E_RBS" or $tp_name eq "DC_E_RAN" or $tp_name eq "DC_E_CPP" ){
					#ERBS & RBS & RAN
					#$time_for_filename = $datetime_hour+int(-2).$datetime_min;
	if(int($datetime_hour-2) < 10){
		$time_for_filename="0".(int($datetime_hour-2))."$datetime_min";}
		else{
		$time_for_filename=(int($datetime_hour-2))."$datetime_min";}
				}
				
				if ($package=~ m/(INTF_\w*)/){
				
				print "This testcase is not applicable for Interfaces.";				
				
				}
				else
				{
				print "$package\n";
				
				$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
				$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left><font color=blue><font size=5px><b>$package</b></font></font></td></tr>";
				
				
				my $datetimeid_10 = $date." 10:00:00";
				my $datetimeid_11 = $date." 11:00:00";
				####Get FDN Data and Validating
				my $query = "select INTERFACENAME from InterfaceTechpacks where TECHPACKNAME LIKE '".$tp_name."'";
				my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
				for my $r ( @$result_from_repdb ) 
				{
					#print "r\n";
					$query = "select COLLECTION_SET_ID from META_COLLECTION_SETS WHERE COLLECTION_SET_NAME LIKE '".@$r[0]."-eniq_oss_1'";
					my @result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
					#print $result_from_repdb1[0];
					$query = "select substr(action_contents_01,charindex('processedDir',action_contents_01)+39,charindex('/processed',action_contents_01)-(charindex('processedDir',action_contents_01)+39)) procdir from meta_transfer_actions where collection_set_id = ".$result_from_repdb1[0]."and action_type like 'parse'";
					@result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
					#print $result_from_repdb1[0];
					my $in_path="/eniq/archive/eniq_oss_1/".$result_from_repdb1[0]."/processed";
					#print "final path is $in_path\n";
					chdir $in_path or die $!;
					#my $correct_intf_file;
					opendir ( DIR, $in_path ) || die "Error in opening dir due to $! \n";
					while( my $intf_file = readdir(DIR)) 
					{
						my ($correct_intf_file) = $intf_file =~ m/(INTF_\w*\_$date_for_filename\.$time_for_filename\.txt)/;
						#print "$intf_file\n\n";
						#print "$correct_intf_file\n";
						if ($intf_file eq $correct_intf_file) 
						{
													
							open INTFILE, '<', $intf_file or die "Could not open '$intf_file', No such file found in the provided path $!\n";
							while(<INTFILE>)
							{
							my $filename = $_;
							# my ($na,$sn,$na1) = $filename =~ m/(.*)(_SubNetwork.*)(_\w*)/;
							# $sn=substr($sn,1);
							my ($t,$node,$temp4)=$filename=~ m/(.*MeContext=)(.+?)(_$datetime_year.*|,.*|_stats.*|_osscounterfile.*|_BARFIL.*|_MRRFIL.*)/;
							#print "$t  $node  $temp4\n";
							my ($temp,$Net,$temp1) = $filename =~ m/(.*NetworkElement=)(.+?)(_$datetime_year.*|,.*|_stats.*|_osscounterfile.*|_BARFIL.*|_MRRFIL.*)/;
							#print "$temp  $Net  $temp1\n";
							my ($temp2,$Manage,$temp3) = $filename =~ m/(.*ManagedElement=)(.+?)(_$datetime_year.*|,.*|_stats.*|_osscounterfile.*|_BARFIL.*|_MRRFIL.*)/;
							#print "$temp2  $Manage  $temp3\n";
							if($node ne "")
							{$FNODE=$node;}
							elsif($Manage ne "")
							{$FNODE=$Manage;}
							else 
							{$FNODE=$Net;}
							#print "$FNODE          $Nodename\n";
							$flag=0;
							
							if ($FNODE eq $Nodename)
							{
								print "File Nodename : $FNODE\n";
								$flag=1;
								my $query="select DATAFORMATTYPE from DataInterface where INTERFACENAME like '@$r[0]'";
								#print "$query";
								my @result_from_repdb1= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ROW");
								my $DataFormat = $result_from_repdb1[0];
								$query = "select distinct substr(substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3),1,charindex(':',substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3))-1) from defaulttags WHERE DATAFORMATID like '%$package%$DataFormat';";
								my $res = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
								
								my @Tables = ();
								for my $data (@$res)
								{
									push(@Tables,@$data[0]);
								}
								TopoData($DataFormat,@$r[0]);
								#print "$TopoNODE\n";
								$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
								$result .= "<tr><td align=left><font color=black><font size=2px><b>Interface NAME:</b></font></font></td><td align=left><font color=orange><font size=2px><b>@$r[0]</b></font></font></td></tr><tr><td align=left><font color=black><font size=2px><b>File NodeName</b></font></font></td><td align=left><font color=orange><font size=2px><b>$FNODE</b></font></font></td></tr>";
								$result_no.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
								$result_no.= "<tr><td align=left><font size=3px><font color=magenta><b>NO DATA IN TABLE FOR DATE $datetime:</b></font></font></td></tr>";
								
								foreach my $Table ( @Tables )
								{
									my @DBNODE=();
									$table_name = $Table."_RAW";
									chomp($table_name);
									#print "$table_name\n";
									$query = "select count(*) from $table_name where OSS_ID like 'eniq_oss_1' and (DATETIME_ID ='$datetimeid_10' OR DATETIME_ID ='$datetimeid_11')";
									my @result_from_dwhdb= executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ROW");
									if ( $result_from_dwhdb[0] eq 0 )
									{
										#print "No data in table\n";
										$empty_num++;
										$result_no.= "<tr><td align=left><font color=000000><b>$table_name</b></font></td></tr>";
									}
					
									else
									{				
									$query = "select distinct $Nodenamekey from $table_name where OSS_ID like 'eniq_oss_1' and (DATETIME_ID ='$datetimeid_10' OR DATETIME_ID ='$datetimeid_11')";
									my $result_from_dwhdb= executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
									for my $r1 (@$result_from_dwhdb)
									{
									push(@DBNODE,@$r1[0]);
									}
									$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left><font color=blue><font size=3px><b>$table_name</b></font></font></td></tr>";
									$result .= "<tr><td align=left><font color=888800><font size=2px><b>Check</b></font></font></td><td align=left><font color=888800><font size=2px><b>RESULT</b></font></font></td></tr>";
									if(Contains($FNODE,\@DBNODE) eq 0)
									{
										#print "Fail\n";
										$num++;
										$result .= "<tr><td align=left><font color=000000><b>File $Nodenamekey compared with Table $Nodenamekey</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
										#print "FAIL\n";
										$num++;
										$result .= "<tr><td align=left><font color=000000><b>Topology $TopoNodeNamekey compared with Table $Nodenamekey</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
									}
									else
									{
										#print "Pass\n";
										$result .= "<tr><td align=left><font color=000000><b>File $Nodenamekey compared with Table $Nodenamekey</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
										$num1++;
										if(Contains($FNODE,\@TopoNODE) eq 0)
										{
											#print "FAIL\n";
											$num++;
											$result .= "<tr><td align=left><font color=000000><b>Topology $TopoNodeNamekey compared with Table $Nodenamekey</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
										}
										else
										{
											#print "PASS\n";
											$result .= "<tr><td align=left><font color=000000><b>Topology $TopoNodeNamekey compared with Table $Nodenamekey</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
											$num1++;
										}
									}
								}
								}
								$result .= "</table>";
							}
							if ($flag == 1){last;}
							}
							last;
						}
					}
					if ($flag eq 1){last;}
				}
			}
		}
		#################################################
$result.=$result_no;
return $result;

}
############################################################################################################################
print "********Executing Keys Validation Script********\n";
my $stime = getTime();
my $result1=NodeNameValidation();
$report =getHtmlHeader();
$report.="<h1> <font color=MidnightBlue><center> <u> Node Name Key Data Validation </u> </font> </h1>";
	
$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
$report.= "<tr>";
$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
$report.= "<td><b>$stime\t</td>";
$report.= "<tr>";
$report.= "<td><font size = 2 ><b>END TIME:\t</td>";
my $etime = getTime();
$report.= "<td><b>$etime\t</td>";

$report.= "<tr>";
$report.= "<td><font size = 2 ><b>RESULT:\t</td>";
$report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($num1) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($num)/ <a href=\"#t3\"><font size = 2 color=magenta><b>No Data in table ($empty_num)</td>";
if($num1 eq 0 and $num eq 0)
{
	print "NodeName_Key_Validation:PASS- -1 FAIL- -1\n";
}
else{
print "NodeName_Key_Validation:PASS- $num1 FAIL- $num No_Data- $empty_num\n";
}
$report.= "</table>";
$report.= "<br>";
 
 #$result.="<h2> $result1 </h2>";
$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
$report.= "<tr>";
$report.= "<td><font size = 2 ><b>$result1\t</td>";
$report.= "</table>";
	 
$report.= getHtmlTail();
my $file = writeHtml("NodeName_KeyData_Validation_".$datenew,$report);
#print "PARTIAL FILE: $file\n"; 
print "********Keys Validation Script is done********\n";