#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
#use Text::CSV;
#use IO::Uncompress::Gunzip qw($GunzipError);
#use IO::File;
#use File::Basename;
#use XML::Simple;


my $result;
my ($num,$num1);
my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$Toponodename,$hyphen1,$month,$hyphen2,$date,$space,$colon1,$colon2,$datetimeepfg,$package);
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
sub Dynamic{

	my %hash_for_mapping_table_counter;
	my %hash_for_mapping_table_tag;
	my %hash_for_mapping_table_counter1;
	my $table_name;
	my $counters;
	my $fnum = 0;
	my ($len,$len1,$k);
	
	
	my $file = 'data_new.txt';
	open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
	my @array = <FILE>;
	my $line = shift @array;
	chomp $line;	
	#print "$line\n";
	
	
	($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$Toponodename) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.*)/;
	($year,$hyphen1,$month,$hyphen2,$date,$space,$hour,$colon1,$min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;
	@array = split(",",$Nodename);
	$Nodename = $array[0];
	$datetimeepfg = "$year$month$date.$hour$min";
	#print "$datetimeepfg";
	$package = $tp_name.":((".$build."))";
	print "$package\n";

	$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr></table>";

	
	my $query = "select distinct substr(substr(DATAFORMATID,charindex('))',DATAFORMATID)+3),1,charindex(':',substr(DATAFORMATID,charindex('))',DATAFORMATID)+3))-1) as tab, dataid as counter, dataname from dataitem where  DATAFORMATID like '$package%DYN' and dataname in (select dataname from measurementcolumn where mtableid like '%'+tab+'%' and coltype like 'COUNTER') order by tab;";
	my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	for my $data_from_repdb (@$result_from_repdb) 
	{
		my ($table_name, $counter_name,$dataid) = @$data_from_repdb;
		my $new_counter = "cast(".$dataid." as Numeric(20,0))";
		push @{$hash_for_mapping_table_counter{$table_name}}, $counter_name;
		push @{$hash_for_mapping_table_counter1{$table_name}}, $new_counter;
	}
	
	
	$query = "select substr(substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3),1,charindex(':',substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3))-1),tagid from defaulttags where dataformatid like '%DYN'";
	$result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	for my $data_from_repdb (@$result_from_repdb)
	{
		my ($table,$tagid) = @$data_from_repdb;
		$hash_for_mapping_table_tag{$tagid}=$table;
	}
	
	$query = "select INTERFACENAME from datainterface where INTERFACENAME in (select INTERFACENAME from interfacetechpacks where techpackname like '$tp_name') and dataformattype like '%DYN%'";
	#print "$query\n";
	$result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	for my $r ( @$result_from_repdb ) 
	{
		#print "@$r[0]\n";
		$query = "select COLLECTION_SET_ID from META_COLLECTION_SETS WHERE COLLECTION_SET_NAME LIKE '".@$r[0]."-eniq_oss_1'";
		my @result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
		#print $result_from_repdb1[0];
		$query = "select substr(action_contents_01,charindex('processedDir',action_contents_01)+39,charindex('/processed',action_contents_01)-(charindex('processedDir',action_contents_01)+39)) procdir from meta_transfer_actions where collection_set_id = ".$result_from_repdb1[0]."and action_type like 'parse'";
		@result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
		#print $result_from_repdb1[0];
		my $dir = $result_from_repdb1[0];
		#print "$dir";
		
		my $in_path = "/eniq/home/dcuser/epfg/txt/$dir";
		if(opendir ( DIR, $in_path ))
		{
		while( my $intf_file = readdir(DIR))
		{
		my ($correct_intf_file) = $intf_file =~ m/(CountersTxt_$datetimeepfg\_$Nodename\.csv)/;
		
		if ($intf_file eq $correct_intf_file) 
		{
		#print "$correct_intf_file\n";
		$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
		$result .= "<tr><td align=left><font color=black><font size=3px><b>Interface Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>@$r[0]</font></font></td></tr>";
		my $csvfile_from_epfg = "/eniq/home/dcuser/epfg/txt/$dir/$correct_intf_file";
		open (EPFG_FILE , '<', $csvfile_from_epfg ) or die "ERROR: Couldn't open file for writing '$csvfile_from_epfg' $! \n";
		#print "$ne_name\n";
		while (<EPFG_FILE>) 
		{
			chomp;	
			my %EpfgDynamic;
			my ($flag);
			my @DBTable;
			my $temptag = '';
			my @epfg_counter_and_data = split(/;/, $_);
			my ($tag,$tagid) = split(":",$epfg_counter_and_data[0]);
			#print Dumper(\keys %hash_for_mapping_table_tag);
			if($tagid eq "ethernet")
			{
				push @DBTable,$hash_for_mapping_table_tag{Traffic};
				push @DBTable,$hash_for_mapping_table_tag{Bandwidth};
				push @DBTable,$hash_for_mapping_table_tag{Delay};
				$flag = 1;
			}
			else
			{
			for my $DBTag (keys %hash_for_mapping_table_tag)
			{
				#print "$DBTag  $tagid\n";
				if($tagid =~ m/(\w*$DBTag\w*)/i)
				{
					push @DBTable,$hash_for_mapping_table_tag{$DBTag};
					$flag = 1;
					last;
				}
			}
			}
			if($flag eq 1)
			{
				my ($mo,$moid) = $epfg_counter_and_data[1] =~ m/(MOID:)(.*)/;
				#print "$moid\n";
				my $count = @epfg_counter_and_data;
				#print "CSV count : $count\n";
				for(my $i=2;$i<$count;$i++)
				{
					my ($DYNcounter,$value) = split(":",$epfg_counter_and_data[$i]);
					$EpfgDynamic{$DYNcounter} = $value;
				}
				my $TableCount = @DBTable;
				#print "$TableCount\n";
				for(my $i=0;$i<$TableCount;$i++)
				{
				my $DBT = $DBTable[$i];
				#print "$DBT\n";
				if($temptag ne $tagid)
				{
					$counters = join(',',@{$hash_for_mapping_table_counter1{$DBT}});
					$len =@{$hash_for_mapping_table_counter{$DBT}};
					#$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					$result .= "<tr><td align=left><font color=black><font size=2px><b>Table Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=2px><b>$DBT</font></font></td></tr>";
					my $query = "select distinct substr(substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3),1,charindex(':',substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3))-1) as tab, DATANAME as keys from MeasurementColumn where MTABLEID like '".$package."%$DBT%:RAW' and COLTYPE like '%KEY' and DATANAME in ('Trans_mode') order by tab;";
					#print "$query";
					my $res = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
					$len1 = @$res;
					if($len1 ne 0)
					{
						$result.= "<tr><td align=left><font color=blue><font size=2px><b>COUNTER NAME</b></font></font></td><td align=left><font color=blue><font size=2px><b>TransMode</b></font></font></td><td align=left><font color=blue><font size=2px><b>MOID</b></font></font></td><td align=left><font color=blue><font size=2px><b>RESULT</b></font></font></td></tr>";
					}
					else
					{
						$result.= "<tr><td align=left><font color=blue><font size=2px><b>COUNTER NAME</b></font></font></td><td align=left><font color=blue><font size=2px><b>MOID</b></font></font></td><td align=left><font color=blue><font size=2px><b>RESULT</b></font></font></td></tr>";
					}
					if($i == $TableCount-1)
					{
					$temptag = $tagid;
					
					}
				}
				if($len1 ne 0)
				{
					#print "$len\n";
					$query = "select $counters,Trans_mode from $DBT"."_raw where DATETIME_ID = '$datetime' and moid like '$moid' and $Nodenamekey like '$Nodename'";
					#print "$query\n";
					my $result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
					#print Dumper(\@$result_from_dwhdb);
					for my $data_dwhdb (@$result_from_dwhdb)
					{
						# my @tempp=keys %EpfgDynamic;
						# print Dumper(\@tempp);
						$k++;
						#print "$k\n";
						my @temp = @{$hash_for_mapping_table_counter{$DBT}};
						for(my $j=0;$j<$len;$j++)
						{
							my $t = "$temp[$j]@$data_dwhdb[$len]";
							#print "****$EpfgDynamic{$t} eq @$data_dwhdb[$j]\n";
							if(grep(/^$t$/,keys %EpfgDynamic))
							{
								if($EpfgDynamic{$t} eq @$data_dwhdb[$j])
								{
									#print "$t pass\n";
									#$result .= "<tr><td align=left><font color=008000><b>$temp[$j]</b></font></td><td align=left><font color=008000><b>@$data_dwhdb[$len]</b></font></td><td align=left><font color=008000><b>$moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
									$num++;
								}
								else
								{
									#print "$t fail\n";
									$result .= "<tr><td align=left><font color=FF0000><b>$temp[$j]</b></font></td><td align=left><font color=FF0000><b>@$data_dwhdb[$len]</b></font></td><td align=left><font color=FF0000><b>$moid</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
									$num1++;
									$fnum++;
								}
							}
						}
					}
					if($fnum eq 0){
					$result .= "<tr><td align=left><font color=008000><b>All Counters are passed</b></font></td><td align=left><font color=008000><b> </b></font></td><td align=left><font color=008000><b>$moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";}
				}
				else 
				{
					$query = "select $counters from $DBT"."_raw where DATETIME_ID = '$datetime' and moid like '$moid' and $Nodenamekey like '$Nodename'";
					#print "$query\n";
					my $result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
					#print Dumper(\@$result_from_dwhdb);
					for my $data_dwhdb (@$result_from_dwhdb)
					{
						#print "$hash_for_mapping_table_counter{$result_from_repdb[0]}\n";
						my @temp = @{$hash_for_mapping_table_counter{$DBT}};
						#print Dumper(\@temp);
						for(my $j=0;$j<$len;$j++)
						{
							my $t = "$temp[$j]";
							#print "****$EpfgDynamic{$t} eq @$data_dwhdb[$j]\n";
							if(grep(/$t/,keys %EpfgDynamic))
							{
								if($EpfgDynamic{$t} eq @$data_dwhdb[$j])
								{
									#print "$t     pass\n";
									#$result .= "<tr><td align=left><font color=008000><b>$temp[$j]</b></font></td><td align=left><font color=008000><b>$moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
									$num++;
								}
								else
								{
									#print "$t fail\n";
									$result .= "<tr><td align=left><font color=FF0000><b>$temp[$j]</b></font></td><td align=left><font color=FF0000><b>$moid</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
									$num1++;
									$fnum++;
								}
							}
						}
					}
					if($fnum eq 0){
					$result .= "<tr><td align=left><font color=008000><b>All Counters are passed</b></font></td><td align=left><font color=008000><b>$moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";}
				}
			}
			}
			else
			{
				print "$tagid in epfg csv is not matching with Tagids in Repdb\n";
			}
		}
		}}}
	}
return $result;
}
##############################################################################################################################################

print "********Executing DynamicCounter_Validation Script********\n";
	$num=0;
	$num1=0;
	my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> Dynamic Counters Validation </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	$report.= "<td><b>$stime\t</td>";
	 
	my $result1=Dynamic();
	#print "$num\n$num1\n";
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>END TIME:\t</td>";
    my $etime = getTime();
	$report.= "<td><b>$etime\t</td>";
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	$report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($num) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($num1)</td>";
	print "Dynamic_Counter_Data_Validation:PASS- $num FAIL- $num1\n";
	$report.= "</table>";
	$report.= "<br>";
	 
	#$result.="<h2> $result1 </h2>";
	$report.="$result1";
	 
    $report.= getHtmlTail();
    my $file = writeHtml("DynamicCounter_Validation".$datenew,$report);
	#print "PARTIAL FILE: $file\n"; 
    #$dataloadingmo ="false";
print "********DynamicCounter_Validation Test Case is done********\n";