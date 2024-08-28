#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $dataloadingmo = "true";
my $final_result;
my $final_pass = 0;
my $final_fail = 0;
my $empty_tables = 0;
my $final_result_no = 0;
my $num=0;
my $num1=0;			 

my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
open(Pass, "<Passwords.txt");
	my $name = do {local $/; <Pass> };
	my ($DCPass,$DwhrepPass,$EtlrepPass) = split("\n",$name);
	#print "DCPass: $DCPass , DwhrepPass: $DwhrepPass , EtlrepPass: $DwhrepPass";

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
##################################################################################
#             The DATETIME value for the FILENAME of the HTML LOGS               #
##################################################################################

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
  $mon++;
  $year=1900+$year;
my $datenew =sprintf("%4d%02d%02d%02d%02d%02d",$year,$mon,$mday,$hour,$min,$wday);

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


############################################################
# Dataloading MO this will verify whether the data is matching with the EPFG input file.
# 
sub dataloadingmo {
	
    my %hash_for_mapping_table_counter;
	my %Miss_Counter;
	my %hash_for_mapping_table_counter2;
	my %hash_for_mapping_table_tag;
	my %hash_for_mapping_table_counter1;
	my %hash_empty;
	my $table_name;
	my $counters;
	my $fnum = 0;
	my ($len,$len1,$k);
	my $miss_tg = "Missed:";
	my %tables;
	my $erbstime;
	
	
	
	my $file = 'data_new.txt';
	open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
	my @array = <FILE>;
	my $line = shift @array;
	chomp $line;	
	#print "$line\n";
	my $datetime1;
	
	my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$Toponodename) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.*)/;
	my ($year,$hyphen1,$month,$hyphen2,$date,$space,$hour,$colon1,$min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;
	
	@array = split(",",$Nodename);
	$Nodename = $array[0];
	my $datetimeepfg = "$year$month$date.$hour$min";
	#$datetime1 = $datetime;
	#print "Given : $datetime\n";  #2023-10-10 10:00:00 or 12:00:00
	if($tp_name eq "DC_E_ERBS" or $tp_name eq "DC_E_RBS" or $tp_name eq "DC_E_RAN" or $tp_name eq "DC_E_CPP")
	{
	if(int($hour-2) < 10){
		$datetimeepfg="$year$month$date.0".(int($hour-2))."$min";}
		else{
		$datetimeepfg="$year$month$date.".(int($hour-2))."$min";}
		
	}
	print "EPFG time : $datetimeepfg\n";
	my $package = $tp_name.":((".$build."))";
	print "$package\n";
	#print "$tp_name\n";

	my $result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr>";
	
	my $result1.=qq{<center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result1 .= "<tr><td align=left><font color=black><font size=4px><b>Table</b></font></font></td><td align=left colspan=7><font color=black><font size=4px><b>Missed Counters from EPFG/PM file</b></font></font></td></tr>";
	
	my $result2.=qq{<center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result2 .= "<tr><td align=left><font color=black><font size=4px><b>Extra Tags in EPFG/PM file</b></font></font></td>";

	my $result3.=qq{<center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result3 .= "<tr><td align=left><font color=black><font size=4px><b>Table</b></font></font></td><td align=left colspan=7><font color=black><font size=4px><b>MOID</b></font></font></td><td align=left colspan=7><font color=black><font size=4px><b>Extra Counters in EPFG/PM file</b></font></font></td></tr>";
	
	my $result4.=qq{<center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result4 .= "<tr><td align=left><font color=black><font size=4px><b>Missed Tags from EPFG/PM file</b></font></font></td></tr>";
	my $result5.=qq{<center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result5 .= "<tr><td align=left><font color=black><font size=4px><b>Empty Tables</b></font></font></td></tr>";
	
	my $query = "select distinct substr(substr(DATAFORMATID,charindex('))',DATAFORMATID)+3),1,charindex(':',substr(DATAFORMATID,charindex('))',DATAFORMATID)+3))-1) as tab, dataname, dataid, datatype from dataitem where  DATAFORMATID like '$package%' and dataname in (select dataname from measurementcolumn where mtableid like '%$package%' and coltype like 'COUNTER') and tab not like '%#_flex%' ESCAPE '#' order by tab;";
	#print "$query\n";
	my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	for my $data_from_repdb (@$result_from_repdb) 
	{
		my ($table_name, $counter_name,$dataid,$datatype) = @$data_from_repdb;
		my $new_counter;
		if($datatype eq 'numeric'){
		$new_counter = "cast(".$counter_name." as Numeric(20,0))";}
		else
		{
		$new_counter = $counter_name;
		}
		push @{$hash_for_mapping_table_counter{$table_name}}, $dataid;
		push @{$Miss_Counter{$table_name}}, $counter_name;
		push @{$hash_for_mapping_table_counter1{$table_name}}, $new_counter;
		push @{$hash_for_mapping_table_counter2{$table_name}}, $counter_name;
	}
	
	#print Dumper(\%hash_for_mapping_table_counter1);
	
	$query = "select substr(substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3),1,charindex(':',substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3))-1),tagid from defaulttags where dataformatid like '%$package%' and tagid not like '%#_flex%' ESCAPE '#'";
	$result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	for my $data_from_repdb (@$result_from_repdb)
	{
		my ($table,$tagid) = @$data_from_repdb;
		$hash_for_mapping_table_tag{$tagid}=$table;
	}
	my @alltags = keys %hash_for_mapping_table_tag;
	#print Dumper(\sort(@alltags));
	#print Dumper(\%hash_for_mapping_table_tag);
	$query = "select INTERFACENAME from datainterface where INTERFACENAME in (select INTERFACENAME from interfacetechpacks where techpackname like '$tp_name' and interfacename not like '%INTF_DC_E_NR%EVENTS%') and dataformattype not like '%DYN'";
	#print "$query\n";
	$result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	for my $r ( @$result_from_repdb ) 
	{
		print "@$r[0]\n";
		$query = "select COLLECTION_SET_ID from META_COLLECTION_SETS WHERE COLLECTION_SET_NAME LIKE '".@$r[0]."-eniq_oss_1'";
		my @result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
		#print $result_from_repdb1[0];
		$query = "select substr(action_contents_01,charindex('processedDir',action_contents_01)+39,charindex('/processed',action_contents_01)-(charindex('processedDir',action_contents_01)+39)) procdir from meta_transfer_actions where collection_set_id = ".$result_from_repdb1[0]."and action_type like 'parse'";
		@result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
		#print $result_from_repdb1[0];
		my $dir = $result_from_repdb1[0];
		#print "$dir";
		
		my $in_path = "/eniq/home/dcuser/epfg/txt/$dir";
		print "$dir\n";
		if(opendir ( DIR, $in_path ))
		{
		while( my $intf_file = readdir(DIR))
		{
		my ($correct_intf_file) = $intf_file =~ m/(CountersTxt_$datetimeepfg\_$Nodename\.csv)/;
		#print "$intf_file\n$correct_intf_file\n";
		if ($intf_file eq $correct_intf_file) 
		{
		#print "$correct_intf_file\n";
		
		$result .= "<tr><td align=left><font color=black><font size=3px><b>Interface Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>@$r[0]</font></font></td></tr></table>";
		$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
		if($tp_name eq "DC_E_IPTRANSPORT")
		{
		$result .= "<tr>
		<td align=left><font color=black><font size=3px><b>Table Name</b></font></font></td>
		<td align=left><font color=black><font size=3px><b>MOID</font></font></td>
		<td align=left><font color=black><font size=3px><b>QUEUE_NUMBER</font></font></td>
		<td align=left><font color=black><font size=3px><b>Tagid</font></font></td>
		<td align=left><font color=black><font size=3px><b>Result</font></font></td></tr>";
		}
		else
		{
		$result .= "<tr>
		<td align=left><font color=black><font size=3px><b>Table Name</b></font></font></td>
		<td align=left><font color=black><font size=3px><b>MOID</font></font></td>
		<td align=left><font color=black><font size=3px><b>Tagid</font></font></td>
		<td align=left><font color=black><font size=3px><b>Result</font></font></td></tr>";
		}
		my $csvfile_from_epfg = "/eniq/home/dcuser/epfg/txt/$dir/$correct_intf_file";
		open (EPFG_FILE , '<', $csvfile_from_epfg ) or die "ERROR: Couldn't open file for writing '$csvfile_from_epfg' $! \n";
		#print "$ne_name\n";
		my ($Counter_data_measurementCounter,$Key_measuerementcounter);
		while (<EPFG_FILE>) 
		{
				my %hash_for_mapping_measurement_counter;
				chomp;	
				my %EpfgData;
				my $Mo1;
				my @CMV;
				my $flag;
				my @DBTable;
				my $temptag = '';
				my @epfg_counter_and_data = split(/;/, $_);
				my ($tag,$tagid) = split(":",$epfg_counter_and_data[0]);
				my ($Mo,$t,$Moid) = $epfg_counter_and_data[1]=~ m/(MOID)(\:)(.*)/;
				if($tp_name eq 'DC_E_AFG')
				{
					($Mo,$t,$Moid) = $epfg_counter_and_data[1]=~ m/(.*)(\:)(.*)/;
					($Moid,$Mo1) = split(/\./,$Moid);				
				}
				
				#print ("$tagid\n");
				if($tp_name eq 'DC_E_BSS' or $tp_name eq 'DC_E_CMN_STS')
				{
					($Mo1,$Moid) = split(/\./,$Moid);
				}
				#print "$tp_name\n";
				
				
				my $count = @epfg_counter_and_data;
				
				if($tp_name eq 'DC_E_NETOP' or ($tp_name eq 'DC_E_CUDB' and @$r[0] ne 'INTF_DC_E_CCDM')){
				for(my $i=1;$i<$count;$i++)
				{
					my ($counter,$value) = split(":",$epfg_counter_and_data[$i]);
					if($counter eq "REPUNDEFGSM" || $counter eq "REPUNDEF"){
						$counter= "REPUNDEF_1";
				    }
					$EpfgData{$counter} = $value;
				}
				}
				
				else
				{
				for(my $i=2;$i<$count;$i++)
				{
					my ($counter,$value) = split(":",$epfg_counter_and_data[$i]);
					$EpfgData{$counter} = $value;
				}
				}
				
				my $flex="pmFlex";
				my @Miss = grep {!/^$flex/i} keys %EpfgData;
				my $DBT = $hash_for_mapping_table_tag{$tagid};
				my $DBTV = $hash_for_mapping_table_tag{"$tagid"."_V"};
				if( @$r[0] eq 'INTF_DC_E_EVENTS_ERBS_ENM')
				{
				$DBT = $hash_for_mapping_table_tag{"$tagid"."_Events"};
				$DBTV = $hash_for_mapping_table_tag{"$tagid"."_Events_V"};
				}
				my $DBTDYN = $hash_for_mapping_table_tag{"$tagid"."_DYN"};
				
				if($DBT ne '' or $DBTV ne '' or $DBTDYN ne '')
				{
				#print "$tagid -- $DBT -- $DBTV -- $DBTDYN\n";
				if($DBTV ne '')
				{
						my $query_from_measurementcounter = "select DATANAME, COUNTERTYPE from measurementcounter where typeid like '%$DBTV%'";
						#print "$query_from_measurementcounter\n";
						my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_measurementcounter,"ALL");
						for my $counterdata_from_repdb (@$result_from_repdb) 
						{
							($Counter_data_measurementCounter,$Key_measuerementcounter) = @$counterdata_from_repdb;
							$hash_for_mapping_measurement_counter{$Counter_data_measurementCounter} = $Key_measuerementcounter;
							#print "$Counter_data_measurementCounter\n";
							#print "$Key_measuerementcounter\n";
						}
				}
				if($DBT ne '')
				{
				$flag = 0;
				$tables{$DBT} =1;
				$fnum=0;
				my $crazy = $tagid;
				if( @$r[0] eq 'INTF_DC_E_EVENTS_ERBS_ENM')
				{
					$crazy = "$tagid"."_Events";
					@alltags = grep {!/^$crazy$/} @alltags;
				}
				else{@alltags = grep {!/^$crazy$/} @alltags;}
				###############################Scalar#################################################
				#print ("$crazy                 --            $DBT                --              $Moid\n");
				$counters = join(',',@{$hash_for_mapping_table_counter1{$DBT}});
				my @temp1 = @{$hash_for_mapping_table_counter2{$DBT}};
				$len =@{$hash_for_mapping_table_counter{$DBT}};
				#$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
				#$result .= "<tr><td align=left><font color=black><font size=2px><b>Table Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=2px><b>$DBT</font></font></td></tr>";
				if($DBT eq "DC_E_AFG_VIRTUAL_GATEWAY")
				{
					$query = "select $counters from $DBT"."_raw where DATETIME_ID = '$datetime' and $Mo like '$Moid%' and $Nodenamekey like '%$Nodename%' and VIRTUAL_GATEWAY like '$crazy'";
				}
				else
				{
			    if($tp_name eq 'DC_E_NETOP' or ($tp_name eq 'DC_E_CUDB' and @$r[0] ne 'INTF_DC_E_CCDM')){
				$query = "select $counters from $DBT"."_raw where DATETIME_ID = '$datetime' and $Nodenamekey like '$Nodename'";
				}
				else
				{
				$query = "select $counters from $DBT"."_raw where DATETIME_ID = '$datetime' and $Mo = '$Moid' and $Nodenamekey like '$Nodename'";
				}
				}
				
				#print "$query\n";
				my $result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
				#print Dumper(\@$result_from_dwhdb);
				my $length1 = @$result_from_dwhdb;
				#print "$length1\n";
				if($length1 ne 0)
				{
				$flag = 1;
				for my $data_dwhdb (@$result_from_dwhdb)
				{
					#print "$data_dwhdb";
					my @temp = @{$hash_for_mapping_table_counter{$DBT}};
					for(my $j=0;$j<$len;$j++)
					{
						#print "****$EpfgData{$temp[$j]} eq @$data_dwhdb[$j]\n";
						if(grep(/^$temp[$j]$/,keys %EpfgData))
						{
						@{$Miss_Counter{$DBT}} = grep {!/^$temp1[$j]$/} @{$Miss_Counter{$DBT}};
						@Miss = grep {!/^$temp[$j]$/} @Miss;
						#print "$EpfgData{$t} eq @$data_dwhdb[$j]\n";
						if($hash_for_mapping_measurement_counter{$temp[$j]} eq 'CMVECTOR')
						{
							@CMV = split(',',$EpfgData{$temp[$j]});
							if(@$data_dwhdb[$j] ne $CMV[0])
							{
								
								if($tp_name eq "DC_E_IPTRANSPORT")
								{
								$result .= "<tr>
								<td align=left><font color=FF0000><b>$temp[$j] of $DBT</b></font></td>
								<td align=left><font color=FF0000><b>$Moid</b></font></td>
								<td align=left><font color=FF0000><b></b></font></td>
								<td align=left><font color=FF0000><b>$crazy</b></font></td>
								<td align=left><font color=FF0000><b>FAIL</b></font></td>
								</tr>";
								$num1++;
								$fnum++;
								}
								else
								{
								$result .= "<tr>
								<td align=left><font color=FF0000><b>$temp[$j] of $DBT</b></font></td>
								<td align=left><font color=FF0000><b>$Moid</b></font></td>
								<td align=left><font color=FF0000><b>$crazy</b></font></td>
								<td align=left><font color=FF0000><b>FAIL</b></font></td>
								</tr>";
								$num1++;
								$fnum++;
								}
								
								
							}
						}
						elsif($EpfgData{$temp[$j]} eq @$data_dwhdb[$j])
						{
							#print "$temp1[$j] pass\n";
							#$result .= "<tr><td align=left><font color=008000><b>$temp[$j]</b></font></td><td align=left><font color=008000><b>@$data_dwhdb[$len]</b></font></td><td align=left><font color=008000><b>$Moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
							#$num++;
							#print "$num\n";
						}
						else
						{
							#print "$temp1[$j] fail\n";
							#print "$EpfgData{$temp[$j]} eq @$data_dwhdb[$j]\n";
							if($tp_name eq "DC_E_IPTRANSPORT")
								{
								$result .= "<tr>
								<td align=left><font color=FF0000><b>$temp[$j] of $DBT</b></font></td>
								<td align=left><font color=FF0000><b>$Moid</b></font></td>
								<td align=left><font color=FF0000><b></b></font></td>
								<td align=left><font color=FF0000><b>$crazy</b></font></td>
								<td align=left><font color=FF0000><b>FAIL</b></font></td>
								</tr>";
								$num1++;
								$fnum++;
								}
								else
								{
								$result .= "<tr>
								<td align=left><font color=FF0000><b>$temp[$j] of $DBT</b></font></td>
								<td align=left><font color=FF0000><b>$Moid</b></font></td>
								<td align=left><font color=FF0000><b>$crazy</b></font></td>
								<td align=left><font color=FF0000><b>FAIL</b></font></td>
								</tr>";
								$num1++;
								$fnum++;
								}
						}
						}
					}
				}
				}
					if($flag == 0)
					{
						$final_result_no++;
						$hash_empty{$DBT}=1;
					}
					elsif($fnum eq 0)
					{
						$num++;
						#print "$crazy\n";
						#$result .= "<tr><td align=left><font color=008000><b>$DBT</b></font></td><td align=left><font color=008000><b>$Moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
					}
					
				####################################Scalar#################################################
				}
				####################################Vectors################################################
				#DBT
				if( $DBTV ne '')
				{
				$flag = 0;
				#print Dumper(\%hash_for_mapping_measurement_counter);
				my $crazy = "$tagid"."_V";
				
				if( @$r[0] eq 'INTF_DC_E_EVENTS_ERBS_ENM')
				{
					$crazy = "$tagid"."_Events_V";
					@alltags = grep {!/^$crazy$/} @alltags;
				}
				else{@alltags = grep {!/^$crazy$/} @alltags;}
				$tables{$DBTV} =1;
				$fnum=0;
				my %Temp_hash_db_vector;
				
				$counters = join(',',@{$hash_for_mapping_table_counter1{$DBTV}});
				my @temp1 = @{$hash_for_mapping_table_counter2{$DBTV}};
				$len =@{$hash_for_mapping_table_counter{$DBTV}};
				#$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
				#$result .= "<tr><td align=left><font color=black><font size=2px><b>Table Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=2px><b>$DBTV</font></font></td></tr>";
				#print "$query\n";
				
				if($tp_name eq 'DC_E_NETOP' or ($tp_name eq 'DC_E_CUDB' and @$r[0] ne 'INTF_DC_E_CCDM'))
				{
				$query = "select $counters,dcvector_index from $DBTV"."_raw where DATETIME_ID = '$datetime' and $Nodenamekey like '$Nodename'";
				}
				else{
				$query = "select $counters,dcvector_index from $DBTV"."_raw where DATETIME_ID = '$datetime' and $Mo = '$Moid' and $Nodenamekey like '$Nodename'";
				}
				
				my $result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
				#print Dumper(\@$result_from_dwhdb);
				my $length1 = @$result_from_dwhdb;
				if($length1 ne 0)
				{
				$flag = 1;
				for my $data_dwhdb (@$result_from_dwhdb)
				{
					#print "$data_dwhdb";
					my @temp = @{$hash_for_mapping_table_counter{$DBTV}};
					for(my $j=0;$j<$len;$j++)
					{	
						my $Just = $temp1[$j];
						@{$Miss_Counter{$DBTV}} = grep {!/^$Just$/} @{$Miss_Counter{$DBTV}};
						if($hash_for_mapping_measurement_counter{$temp[$j]} eq "COMPRESSEDVECTOR" and @$data_dwhdb[$j] ne '')
						{
							push @{$Temp_hash_db_vector{$temp[$j]}}, @$data_dwhdb[$len];
							push @{$Temp_hash_db_vector{$temp[$j]}}, @$data_dwhdb[$j];
						}
						elsif(@$data_dwhdb[$j] ne '')
						{
							push @{$Temp_hash_db_vector{$temp[$j]}}, @$data_dwhdb[$j];
						}
					}
				}
				for my $counter (keys %Temp_hash_db_vector) 
				{
						#print "****$EpfgData{$temp[$j]} eq @$data_dwhdb[$j]\n";
						if(grep(/^$counter$/,keys %EpfgData))
						{
						@Miss = grep {!/^$counter$/} @Miss;
						my $Fun = join(',',@{$Temp_hash_db_vector{$counter}});
						my $count = @{$Temp_hash_db_vector{$counter}};
						if($hash_for_mapping_measurement_counter{$counter} eq "COMPRESSEDVECTOR")
						{
							$Fun = ${$Temp_hash_db_vector{$counter}}[$count-2]+1 .",$Fun";
						}
						if($hash_for_mapping_measurement_counter{$counter} eq "CMVECTOR")
						{
							@CMV = split(',',$EpfgData{$counter});
							$EpfgData{$counter} = join(',',@CMV[1..scalar(@CMV)-1]);
						}
						if($EpfgData{$counter} eq $Fun)
						{
							#print "$counter pass\n";
							#$result .= "<tr><td align=left><font color=008000><b>$counter</b></font></td><td align=left><font color=008000><b>$Moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
							#$num++;
							#print "$num\n";
						}
						else
						{
							#print "$hash_for_mapping_measurement_counter{$counter}\n";
							#print  "$EpfgData{$counter} eq $Fun\n";
							#print "$counter fail\n";
							if($tp_name eq "DC_E_IPTRANSPORT")
								{
								$result .= "<tr>
								<td align=left><font color=FF0000><b>$counter of $DBTV</b></font></td>
								<td align=left><font color=FF0000><b>$Moid</b></font></td>
								<td align=left><font color=FF0000><b></b></font></td>
								<td align=left><font color=FF0000><b>$crazy</b></font></td>
								<td align=left><font color=FF0000><b>FAIL</b></font></td>
								</tr>";
								$num1++;
								$fnum++;
								}
								else
								{
								$result .= "<tr>
								<td align=left><font color=FF0000><b>$counter of $DBTV</b></font></td>
								<td align=left><font color=FF0000><b>$Moid</b></font></td>
								<td align=left><font color=FF0000><b>$crazy</b></font></td>
								<td align=left><font color=FF0000><b>FAIL</b></font></td>
								</tr>";
								$num1++;
								$fnum++;
								}
						}
						}
				}
				}
					if($flag == 0)
					{
						$final_result_no++;
						$hash_empty{$DBTV}=1;
					}
					elsif($fnum eq 0)
					{
						$num++;
						#print "$crazy\n";
						#$result .= "<tr><td align=left><font color=008000><b>$DBTV</b></font></td><td align=left><font color=008000><b>$Moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
					}
				}
				#############################################Vectors################################################
				if($DBTDYN ne '')
				{
				$flag = 0;
					$tables{$DBTDYN} =1;
					$fnum=0;
					my $crazy = $tagid."_DYN";
					@alltags = grep {!/^$crazy$/} @alltags;
					$counters = join(',',@{$hash_for_mapping_table_counter1{$DBTDYN}});
					my @temp1 = @{$hash_for_mapping_table_counter2{$DBTDYN}};
					$len =@{$hash_for_mapping_table_counter{$DBTDYN}};
					$query = "select $counters,QUEUE_NUMBER from $DBTDYN"."_raw where DATETIME_ID = '$datetime' and $Mo like '$Moid%' and $Nodenamekey like '%$Nodename%'";
					#print "$query\n";
					my $result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
					#print Dumper(\@$result_from_dwhdb);
					my $length1 = @$result_from_dwhdb;
					#print "\n$length1\n";
					if($length1 ne 0)
					{
						$flag = 1;
						for my $data_dwhdb (@$result_from_dwhdb)
						{
							#print "$data_dwhdb";
							my @temp = @{$hash_for_mapping_table_counter{$DBTDYN}};
							for(my $j=0;$j<$len;$j++)
							{
								$temp[$j] = "queue@$data_dwhdb[$len]"."_$temp[$j]";
								#print "$temp[$j]\n";
								#print "****$EpfgData{$temp[$j]} eq @$data_dwhdb[$j]\n";
								if(grep(/^$temp[$j]$/,keys %EpfgData))
								{
									@{$Miss_Counter{$DBTDYN}} = grep {!/^$temp1[$j]$/} @{$Miss_Counter{$DBTDYN}};
									@Miss = grep {!/^$temp[$j]$/} @Miss;
									if($EpfgData{$temp[$j]} eq @$data_dwhdb[$j])
									{
										#print "$temp1[$j] pass\n";
										#$result .= "<tr><td align=left><font color=008000><b>$temp[$j]</b></font></td><td align=left><font color=008000><b>@$data_dwhdb[$len]</b></font></td><td align=left><font color=008000><b>$Moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
										#$num++;
										#print "$num\n";
									}
									else
									{
										#print "$temp1[$j] fail\n";
										#print $Outputfile_from_script "$EpfgData{$temp[$j]} eq @$data_dwhdb[$j]\n";
										if($tp_name eq "DC_E_IPTRANSPORT")
										{
										$result .= "<tr>
										<td align=left><font color=FF0000><b>$temp[$j] of $DBTDYN</b></font></td>
										<td align=left><font color=FF0000><b>$Moid</b></font></td>
										<td align=left><font color=FF0000><b>@$data_dwhdb[$len]</b></font></td>
										<td align=left><font color=FF0000><b>$crazy</b></font></td>
										<td align=left><font color=FF0000><b>FAIL</b></font></td>
										</tr>";
										$num1++;
										$fnum++;
										}
										else
										{
										$result .= "<tr>
										<td align=left><font color=FF0000><b>$temp[$j] of $DBTDYN</b></font></td>
										<td align=left><font color=FF0000><b>$Moid</b></font></td>
										<td align=left><font color=FF0000><b>$crazy</b></font></td>
										<td align=left><font color=FF0000><b>FAIL</b></font></td>
										</tr>";
										$num1++;
										$fnum++;
										}
									}
								}
							}
						}
					}
					if($flag == 0)
					{
						$final_result_no++;
						$hash_empty{$DBTDYN}=1;
					}
					elsif($fnum eq 0)
					{
						$num++;
						#print "$crazy\n";
						#$result .= "<tr><td align=left><font color=008000><b>$DBTDYN</b></font></td><td align=left><font color=008000><b>$Moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
					}
					
				}
				$len = @Miss;
				if($len gt 0)
				{
					if($DBT ne ''){
					my $temp = join(' , ',@Miss);
					$result3 .= "<tr><td align=left><font color=black><b>$DBT</b></font></td><td align=left colspan=7 width = 50%><font color=black><b>$Moid</b></font></td><td align=left colspan=7 width =50%><font color=800080><b>$temp</b></font></td></tr>";}
					elsif($DBTV ne ''){
					my $temp = join(' , ',@Miss);
					$result3 .= "<tr><td align=left><font color=black><b>$DBTV</b></font></td><td align=left colspan=7 width = 50%><font color=black><b>$Moid</b></font></td><td align=left colspan=7 width =50%><font color=800080><b>$temp</b></font></td></tr>";}
					else{
					my $temp = join(' , ',@Miss);
					$result3 .= "<tr><td align=left><font color=black><b>$DBTDYN</b></font></td><td align=left colspan=7 width = 50%><font color=black><b>$Moid</b></font></td><td align=left colspan=7 width =50%><font color=800080><b>$temp</b></font></td></tr>";}
				}
				
			}
			else
			{
				$query = "select count(*) from defaulttags where tagid like '$tagid'";
				#print "$query\n";
				my @result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ROW");
				if($result_from_repdb[0] eq 0){
				$result2 .= "<tr><td align=left><font color=800080><b>$tagid</b></font></td></tr>";}
			}
	}
	}
	}
	}
	else
	{
		print "Unable to open $dir\n";
	}
	}
	
	for my $keyy (keys %Miss_Counter)
	{
	my @temp1 = @{$Miss_Counter{$keyy}};
	my @temp2 = @{$Miss_Counter{$keyy}};
	my $f=0;
	for my $tag (@alltags)
	{
		if($hash_for_mapping_table_tag{$tag} eq $keyy)
		{
		$f=1;
		}
	}
	if(grep(/^$keyy$/,keys %hash_empty))
	{
		$f=1;
	}
	if($f eq 0)
	{
	for my $i (@temp1)
	{
	$query = "select count(*) from measurementcounter where typeid like '%$package%' and dataname like '$i' and (followjohn = 0 or description like '%deprecated%')";
	#print "$query\n";
	my @result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ROW");
	if($result_from_repdb[0] gt 0)
	{
		@temp2 = grep {!/^$i$/} @temp2;
	}
	}
	my $miss_count = @temp2;
	if($miss_count ne 0)
	{
		my $temp = join(',',@temp2);
		$result1 .= "<tr><td align=left width=50%><font color=black><b>$keyy</b></font></td><td align=left colspan=7 width=50%><font color=8B4513><b>$temp</b></font></td></tr>";
	}
	}
	}
	my $tags = join(',  ',@alltags);
	$result4 .= "<tr><td align=left width=50%><font color=Magenta><b>$tags</b></font></td></tr>";
	$result .= "</table>";
	$tags = join(",  ",keys %hash_empty);
	$result5 .= "<tr><td align=left width=50%><font color=Magenta><b>$tags</b></font></td></tr>";
	$final_result .= "$result<br></br>";
	$final_result .= "$result1<br></br>";
	$final_result .= "$result5<br></br>";
	$final_result .= "$result4<br></br>";
	$final_result .= "$result2<br></br>";
	$final_result .= "$result3<br></br>";
	
	
	#print "$miss_tg\n";
	#print "$miss_count\n";
	#$result .= "<tr><td align=left><font color=008000><b>$temp[$j]</b></font></td><td align=left><font color=008000><b>@$data_dwhdb[$len]</b></font></td><td align=left><font color=008000><b>$Moid</b></font></td><td align=left><font color=008000><b>PASS</b></font></td></tr>";
	my $leng=keys %tables;
	
	#print "$leng\n";
	my @return_result;
	push @return_result, $final_result;
	push @return_result, $tp_name;
	return @return_result;
}
###################################################################################################################################################################

if($dataloadingmo eq "true")
   {
	print "********Counter Data Validation Started********\n";
     my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> COUNTER DATA VALIDATION </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my ($result1,$tp)=dataloadingmo();
	    	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>TABLE PASS: $num / <a href=\"#t2\"><font size = 2 color=red><b>Counters FAIL: $num1 / <a href=\"#t2\"><font size = 2 color=red> No Data: $final_result_no";
	 print "Counter_Data_Validation:PASS- $num FAIL- $num1 No Data- $final_result_no\n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 $report.="$result1";
	 	 
     $report.= getHtmlTail();
     my $file = writeHtml("COUNTER_DATA_VALIDATION-$tp-".$datenew,$report);
	 print "********Counter Validation test case executed successfully.********\n"; 
     $dataloadingmo ="false";
   }