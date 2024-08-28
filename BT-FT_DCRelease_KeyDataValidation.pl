#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $result;
my $report="";
my $num = 0;
my $result_no;
my $empty_num = 0;
my $num1 = 0;
my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNamekey);
my $package;
my %TopoDC;
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
return qq{<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<title>
DC_RELEASE Key Data Validation
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
		 elsif ( $type eq "HASH" ) {
                $sel_stmt->execute() or warn $DBI::errstr;
                my $result = $sel_stmt->fetchrow_hashref();
                $sel_stmt->finish();
                return $result;
        }
        $dbh->disconnect;
}


sub TopoData
{
	####Get Topo Data
	my $DataFormat = $_[0];
	
	my $query="select distinct substr(substr(config,charindex('sql=',config)+4),0,charindex('basetable',substr(config,charindex('sql=',config)+4))-1) as c from transformation where transformerid like '%$package:%$DataFormat' and config like '%version from%'"; 
	#and config like '%basetable%' and c not like '%SITE' and c not like 'DIM_TIMELEVEL'";
	my $result_from_repdb1= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	for my $r (@$result_from_repdb1)
	{
	$query = "@$r[0] where oss_id like 'eniq_oss_1'";
	my $result_from_dwhdb= executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
	for my $i (@$result_from_dwhdb)
	{
		my ($oss,$node) = split(":",@$i[0]);
		$TopoDC{$node} = @$i[1];
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
##################################################################################################################################

sub DC_RValidation{

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
				my $datetimeepfg = $date_for_filename.".".$time_for_filename;

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
				my @arr = split("_",$tp_name);
				
				$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
				$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left><font color=blue><font size=5px><b>$package</b></font></font></td></tr>";
				$result_no.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
				$result_no.= "<tr><td align=left><font size=3px><font color=magenta><b>NO DATA IN TABLE FOR DATE $datetime:</b></font></font></td></tr>";
				
				my $datetimeid_10 = $date." 10:00:00";
				my $datetimeid_11 = $date." 11:00:00";
				####Get FDN Data and Validating
				my $query = "select INTERFACENAME from InterfaceTechpacks where TECHPACKNAME LIKE '".$tp_name."'";
				my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
				for my $r ( @$result_from_repdb ) 
				{
					#print "@$r[0]\n";
					$query = "select COLLECTION_SET_ID from META_COLLECTION_SETS WHERE COLLECTION_SET_NAME LIKE '".@$r[0]."-eniq_oss_1'";
					my @result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
					#print $result_from_repdb1[0];
					$query = "select substr(action_contents_01,charindex('processedDir',action_contents_01)+39,charindex('/processed',action_contents_01)-(charindex('processedDir',action_contents_01)+39)) procdir from meta_transfer_actions where collection_set_id = ".$result_from_repdb1[0]."and action_type like 'parse'";
					@result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
					#print $result_from_repdb1[0];
					if(@$r[0] eq "INTF_DC_E_SHARED_CNF")
					{
					my $dir ="/eniq/home/dcuser/epfg/txt/$result_from_repdb1[0]";
					#if (opendir ( DIR, $dir )){
					my $csvfile_from_epfg = "/eniq/home/dcuser/epfg/txt/$result_from_repdb1[0]/CountersTxt_$datetimeepfg"."_$Nodename.csv";
					#print "$csvfile_from_epfg\n";
					$query = "select distinct substr(config,charindex('=',config)+1) from Transformation where transformerid like '%$tp_name%' and target like 'DC_RELEASE'";
					@result_from_repdb1 = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ROW");
					#print "$result_from_repdb1[0]\n";
					open (EPFG_FILE , '<', $csvfile_from_epfg ) or die "ERROR: Couldn't open file for writing '$csvfile_from_epfg' $! \n";
					$query = "select distinct substr(substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3),1,charindex(':',substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3))-1),tagid from defaulttags WHERE DATAFORMATID like '%$package%';";
					$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					$result .= "<tr><td align=left><font color=black><font size=2px><b>Interface NAME</b></font></font></td><td align=left><font color=orange><font size=2px><b>@$r[0]</b></font></font></td></tr>";
					$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left><font color=blue><font size=3px><b>DC_Release from the Script</b></font></font></td><td align=left><font color=blue><font size=3px><b>RESULT</b></font></font></td></tr>";
					my $res = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
					my %hash_tagid;
					for my $i(@$res)
					{
						$hash_tagid{@$i[1]}=@$i[0];
					}
					while(<EPFG_FILE>)
					{
					my @epfg_data = split(/;/, $_);
					my ($tag,$tagid) = split(":",$epfg_data[0]);
					my ($Mo,$Moid) = split(":",$epfg_data[1]);
					if(grep /$tagid/, keys %hash_tagid)
					{
					my @dc=$Moid=~ m/$result_from_repdb1[0]/;
					#print "$dc[0]\n";
					if($dc[0] ne "")
					{
					$query = "select distinct DC_RELEASE from $hash_tagid{$tagid}"."_raw where OSS_ID like 'eniq_oss_1' and (DATETIME_ID ='$datetimeid_10' OR DATETIME_ID ='$datetimeid_11') and $Nodenamekey like '$Nodename' and DC_RELEASE like '$dc[0]'";
					#print "$query\n";
					my @result_from_dwhdb= executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ROW");
					my $len =@result_from_dwhdb;
					if($len < 0)
					{
						#print "Fail\n";
						$num++;
						$result .= "<tr><td align=left><font color=000000><b>$hash_tagid{$tagid}</b></font></td><td align=left><font color=000000><b>$dc[0]</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
					}
					else
					{
						#print "Pass\n";
						$result .= "<tr><td align=left><font color=000000><b>$hash_tagid{$tagid}</b></font></td><td align=left><font color=000000><b>$dc[0]</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
						$num1++;
					}
					}
					else
					{
						$num++;
						$result .= "<tr><td align=left><font color=000000><b>$hash_tagid{$tagid}</b></font></td><td align=left><font color=000000><b>$dc[0]</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
					}
					}
					
					}
					last;
					}
					else
					{
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
							my ($na,$sn,$na1) = $filename =~ m/(.*)(_SubNetwork.*)(_stats\w*|_osscounterfile.*|_BARFIL.*|_MRRFIL.*)/;
							my $FSN=substr($sn,1);			
							my ($t,$node,$temp4)=$filename=~ m/(.*MeContext=)(.+?)(_$datetime_year.*|,.*|_stats.*|_osscounterfile.*|_BARFIL.*|_MRRFIL.*)/;
							#print "$t,$node,$temp4\n";
							my ($temp,$Net,$temp1) = $filename =~ m/(.*NetworkElement=)(.+?)(_$datetime_year.*|,.*|_stats.*|_osscounterfile.*|_BARFIL.*|_MRRFIL.*)/;
							my ($temp2,$Manage,$temp3) = $filename =~ m/(.*ManagedElement=)(.+?)(_$datetime_year.*|,.*|_stats.*|_osscounterfile.*|_BARFIL.*|_MRRFIL.*)/;
							my $FNODE;
							if($node ne "")
							{$FNODE=$node;}
							elsif($Manage ne "")
							{$FNODE=$Manage;}
							else 
							{$FNODE=$Net;}
							
							$flag=0;
							if ($FNODE eq $Nodename)
							{
								print "File Nodename : $FNODE\n";
								$flag=1;
								my $query="select DATAFORMATTYPE from DataInterface where INTERFACENAME like '@$r[0]'";
								my @result_from_repdb1= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ROW");
								my $DataFormat = $result_from_repdb1[0];
								$query = "select distinct substr(substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3),1,charindex(':',substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3))-1) from defaulttags WHERE DATAFORMATID like '%$package%_$DataFormat';";
								my $res = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
								my @Tables = ();
								for my $data (@$res)
								{
									push(@Tables,@$data[0]);
								}
								TopoData($DataFormat);
								#print Dumper(\%TopoDC);;
								$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
								if($TopoDC{$Nodename} ne "")
								{
								$result .= "<tr><td align=left><font color=black><font size=2px><b>Interface NAME</b></font></font></td><td align=left><font color=orange><font size=2px><b>@$r[0]</b></font></font></td></tr><tr><td align=left><font color=black><font size=2px><b>Topology NodeName - Topology Version</b></font></font></td><td align=left><font color=orange><font size=2px><b>$Nodename - $TopoDC{$Nodename}</b></font></font></tr>";
								$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left><font color=blue><font size=3px><b>RESULT</b></font></font></td></tr>";
								}
								else
								{
								$result .= "<tr><td align=left><font color=black><font size=2px><b>Interface NAME</b></font></font></td><td align=left><font color=orange><font size=2px><b>@$r[0]</b></font></font></td></tr><tr><td align=left><font color=black><font size=2px><b>Topology NodeName - Topology Version</b></font></font></td><td align=left><font color=orange><font size=2px><b>$Nodename - $TopoDC{$FSN}</b></font></font></tr>";
								$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left><font color=blue><font size=3px><b>RESULT</b></font></font></td></tr>";
								}
								
								
								foreach my $Table ( @Tables )
								{
									
									$table_name = $Table."_RAW";
									chomp($table_name);
									#print "$table_name\n";
									$query = "select count(*) from $table_name where OSS_ID like 'eniq_oss_1' and (DATETIME_ID ='$datetimeid_10' OR DATETIME_ID ='$datetimeid_11') and $Nodenamekey like '$Nodename'";
									my @result_from_dwhdb= executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ROW");
									
									if ( $result_from_dwhdb[0] eq 0 )
									{
										#print "No data in table\n";
										$empty_num++;
										$result_no.= "<tr><td align=left><font color=000000><b>$table_name</b></font></td></tr>";
									}
					
									else
									{
									$query = "select distinct DC_RELEASE from $table_name where OSS_ID like 'eniq_oss_1' and (DATETIME_ID ='$datetimeid_10' OR DATETIME_ID ='$datetimeid_11') and $Nodenamekey like '$Nodename'";
									#print "$query\n";
									my @result_from_dwhdb= executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ROW");
									#print "$result_from_dwhdb[0]-$TopoDC{$FSN}\n";
									if(($TopoDC{$Nodename} ne $result_from_dwhdb[0]) and ($TopoDC{$FSN} ne $result_from_dwhdb[0]))
									{
										#print "Fail\n";
										$num++;
										$result .= "<tr><td align=left><font color=000000><b>$table_name</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
									}
									else
									{
										#print "Pass\n";
										$result .= "<tr><td align=left><font color=000000><b>$table_name</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
										$num1++;
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
		}
				#################################################
$result.=$result_no;
return $result;
}

############################################################################################################################
print "********Executing DC_RELEASE Key Validation Script********\n";
my $stime = getTime();
my $result1=DC_RValidation();
$report =getHtmlHeader();
$report.="<h1> <font color=MidnightBlue><center> <u> DC_RELEASE Key Data Validation </u> </font> </h1>";
	
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
	print "DC_RELEASE_Key_Validation:PASS- -1 FAIL- -1\n";
}
else{
print "DC_RELEASE_Key_Validation:PASS- $num1 FAIL- $num No_Data- $empty_num\n";
}
$report.= "</table>";
$report.= "<br>";
 
 #$result.="<h2> $result1 </h2>";
$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
$report.= "<tr>";
$report.= "<td><font size = 2 ><b>$result1\t</td>";
$report.= "</table>";
	 
$report.= getHtmlTail();
my $file = writeHtml("DC_RELEASE_KeyData_Validation_".$datenew,$report);
#print "PARTIAL FILE: $file\n"; 
print "********DC_RELEASE Key Validation Script is done********\n";