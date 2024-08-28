#!/usr/bin/perl

#EQEV-121210 -> Added Topo Keys Check
#EQEV-124004 -> Supported static tables and tables that are loaded from external statements of TOPO

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $keysloading = "true";
my $result;
my $report="";
my $num = 0;
my $num1 = 0;
my $empty_num = 0;
my $Skip_Num = 0;
my $result_no;
my $result_external;					

my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";

my $TopoNodeNamekey = $ARGV[0];
my $TopoNodename = join("','",split(",",$ARGV[1]));
#print "$TopoNodenameKey--$TopoNodename";
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
				my $result_Skip;
				my @result;
				my @Dimtp;
				my $file;
				my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4);
				
				
							
				my $time_file = 'time.txt';
				open TIME, '<', $time_file or die "Could not open '$time_file', No such file found in the provided path $!\n";
				my $today = <TIME>;
				#print "$today\n";
				
				if($TopoNodeNamekey eq '')
				{
				$file = 'data_new.txt';
				open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
				@array = <FILE>;
				my $Number = @array;
				$line = ();
				$line = shift @array;
				chomp $line;	
				($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNamekey) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.+)/;
				$Nodename = join("','",split(",",$Nodename));
				#print "Nodename :$Nodename\n";
				}
				
				$file = 'data.txt';
				open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
				@array = <FILE>;
				#print "@array\n";
				my $count = @array;
				#print "$count\n";
				
				for ($i=0;$i<$count;$i++){
				
					undef $line;
					$line = shift @array;
					chomp $line;
					#print "$line\n";
					my ($strings,$r,$b) = $line  =~ m/(\w*_\w*)(\_R.+\_b)(\d+)/;
					#print "$strings\n";
				
					my $package = $strings.":((".$b."))";
				
					if ($strings=~ m/(INTF_\w*)/){
				
						print "This testcase is not applicable for Interfaces.";				
				
					}else{
					foreach my $tp_names ($package){
						#print "$tp_names  : techpacks\n";
						open(KeyFile, "<Ignore_Keys.txt");
						my $Ignore = do {local $/; <KeyFile> };
						#print Dumper(\$Ignore);
						my @Ignore_Keys=split("\n",$Ignore);
						#print Dumper(\@Ignore_Keys);
						my %Ignore_Key;
						for my $Line(@Ignore_Keys)
						{
							chomp($Line);
							my ($Table,$Keys)=split(":",$Line);
							my @Teams = split(",",$Keys);
							for my $Team(@Teams){
							push @{$Ignore_Key{$Table}}, $Team;}
							#print "$Temp1\n";
						}
						
						$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp_names" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
						$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left><font color=blue><font size=5px><b>$tp_names</b></font></font></td></tr>";
						$report.= "</table>";
						$report.= "<br>";
				
						$result_no.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp_names" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
						$result_no.= "<tr><td align=left><font size=3px><font color=magenta><b>NO DATA IN TABLE FOR DATE $today:</b></font></font></td></tr>";
				
						$result_external.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp_names" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
						$result_external.= "<tr><td align=left><font size=3px><font color=magenta><b>Below tables can be checked as a part of External statement verification test case:</b></font></font></td></tr>";
						$result_Skip.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp_names" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
						$result_Skip.= "<tr><td align=left><font size=3px><font color=magenta><b>Skipped ($TopoNodeNamekey didn't Present)</b></font></font></td></tr>";										
				
						#print "The TechPack/Node name is $tp_names \n\n";
						my $query;
						@Dimtp =  split ("_",$strings);
						#print Dumper(\@Dimtp);
						#if($tp_names=~ m/(DC_\w*)/){
				
						if(($Dimtp[0] eq "DC") and ($Dimtp[2] ne "LLE" and $Dimtp[2] ne "IPRAN" and $Dimtp[2] ne "WLE" and $Dimtp[2] ne "FFAXW"  and $Dimtp[2] ne "FFAX")){
							$query = "select distinct substr(substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3),1,charindex(':',substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3))-1) as tab, DATANAME as counter from MeasurementColumn where  MTABLEID like '".$tp_names."%:RAW' and COLTYPE like '%KEY' order by tab;";
						}else{
							$query = "select distinct substr(ReferenceColumn.TYPEID,charindex('))',ReferenceColumn.TYPEID)+3) as tab, DATANAME from ReferenceColumn where TYPEID like '$tp_names%' order by tab;";
						}
						my $res = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
						#print "$query\n";
				
						undef %hash;
						undef %hash1;
						undef %hash_;
						undef %hash1_;
						#print "query::$query\n";
				
						for my $data (@$res){
							my ($table, $counter) = @$data;
							#print "$table\n";
							if(grep(/^$counter$/,@{$Ignore_Key{$table}}))
							{
								#print "$table:$counter\n";
							}
							else
							{
							push @{$hash{$table}}, $counter;
							}
							#print "table names : $table\n";	
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
						my @statictables = ("DIM_E_IPTNMS_PACKET_ENDPOINTS","DIM_E_LLE_BIS","DIM_E_LTE_NRCELLCU_AREA","DIM_E_LTE_NRCELLDU_AREA","DIM_E_VOLTE_CONFIG","DIM_E_WLE_RBS_DL","DIM_E_WLE_RBS_UL","DIM_E_WLE_ACCEPTABLERATES","DIM_E_WLE_RESTRICTED_UL","DIM_E_WLE_MARGCODE_FRAG","DIM_E_WLE_EULCES_ACC_RATE","DIM_E_WLE_BIS","DIM_E_WLE_ACC_RATE","DIM_E_VOWIFI_CONFIG");
				
						my @externaltables = ("DIM_E_CN_MGW_DEVICETYPE","DIM_E_LLE_pdcchCfiMode","DIM_E_LLE_HWCONNECTEDUSERS","DIM_E_VOLTE_QCI","DIM_E_VOLTE_RAN_SAMPLE","DIM_E_WLE_pmOwnUuLoad","DIM_E_WLE_pmHwCePoolEul","DIM_E_WLE_INFO","DIM_E_WLE_pmUsedHsPdschCodes","DIM_E_WLE_CE_LADDER","DIM_E_WLE_LIC_USER","DIM_E_FFAX_BIN","DIM_E_FFAXW_BIN");
				
															
						foreach my $key ( keys %hash ){
						my $pole = "true";	
							undef $table_nam;
							if(($Dimtp[0] eq "DC") and ($Dimtp[2] ne "LLE" and $Dimtp[2] ne "IPRAN" and $Dimtp[2] ne "WLE" and $Dimtp[2] ne "FFAXW" and  		$Dimtp[2] ne "FFAX")){
								$table_nam = $key."_RAW";
							}else{
								$table_nam = $key;
							}
							chomp($table_nam);
							my $len = @{$hash{$key}};
					
							my $query_array = join(',', @{$hash{$key}});
					
							my $datetimeid_10 = $today." 10:00:00";
							my $datetimeid_11 = $today." 11:00:00";
							#print "$table_nam";
							my $query2;
							#if($tp_names=~ m/(DC_\w*)/){
							if(($Dimtp[0] eq "DC") and ($Dimtp[2] ne "LLE" and $Dimtp[2] ne "IPRAN" and $Dimtp[2] ne "WLE" and $Dimtp[2] ne "FFAXW" and $Dimtp[2] ne "FFAX"))
							{
								if($Dimtp[2] ne "BULK")
								{
								$query2 = "select ".$query_array." from ".$table_nam." WHERE (DATETIME_ID ='$datetimeid_10' OR DATETIME_ID ='$datetimeid_11') and $Nodenamekey in ('$Nodename');" ;
								#print "query2 :$query2\n";
								}
								else
								{
								$query2 = "select ".$query_array." from ".$table_nam." WHERE DATETIME_ID ='$datetime' and $Nodenamekey in ('$Nodename');" ;
								}

							}
							else{
								if(grep(/^$table_nam$/,@statictables)){
									$query2 = "select $query_array from $table_nam";
									#my @res1 = executeSQL("dwhdb",2640,"dc",$DCPass,$query2,"ROW");
								}elsif(grep(/^$table_nam$/,@externaltables)){
									#skipping
									$pole= "false";
									$result_external.= "<tr><td align=left><font color=000000><b>$table_nam</b></font></td></tr>";
								}else{
									my $Temp=$hash{$table_nam};
									if(grep(/^$TopoNodeNamekey$/,@$Temp))
									{
									$query2 = " select $query_array from $table_nam where $TopoNodeNamekey in ('$TopoNodename') and modified in (select max(modified) from $table_nam where $TopoNodeNamekey in ('$TopoNodename'));";
									
									}
									else
									{
										$result_Skip .= "<tr><td align=left><font color=000000><b>$table_nam</b></font></td></tr>";
										$Skip_Num++;
										$pole="false";
									}
								}
							}
							
								#print "$query2\n";
								if($pole eq "true"){
									my @res1 = executeSQL("dwhdb",2640,"dc",$DCPass,$query2,"ROW");
									#my $res2 = join(';', @res1);
									#print "$res2\n";
					
									if ( !@res1 ){
										#print "No data in table\n";
										#print "$table_nam\n";
										$empty_num++;
					
										$result_no.= "<tr><td align=left><font color=000000><b>$table_nam</b></font></td></tr>";
							
									}else{
										$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left><font 	color=blue><font size=3px><b>$table_nam</b></font></font></td></tr>";
										$result .= "<tr><td align=left><font color=800000><font size=2px><b>KEY NAME</b></font></font></td><td align=left><font color=800000><b>VALUE</b></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
										#print "$table_nam                $len\n";
										#my @output2 = undef;
										#push (@output2 ,$res2);
						
										my @array11 = split (",",$query_array);
						
										my $count1 = @array11;
										my $a;
										for ($a=0;$a<1;$a++){
						
											my @array22 = @res1;
											#print Dumper(\@array22);
											my $pnum=0;
											my $fnum=0;
											my $count2 = @array22;
											my $b;
											for ($b=0;$b<$count2;$b++){
						
												my $final_result;
												#print "$array22[$b]\n";
												if ("$array22[$b]" eq ''){
						
													$final_result = 'FAIL';
						
												}else {	
						
													$final_result = 'PASS';
												}
												if ($final_result eq 'PASS'){
													$num1++;
													$pnum++;
													$result .= "<tr><td align=left><font color=000000><b>$array11[$b]</b></font></td><td align=left><font color=000000><b>$array22[$b]</b></font></td><td align=left><font color=008000><b>$final_result</b></font></td></tr>";
												}else {
													$num++;
													$fnum++;
													$result .= "<tr><td align=left><font color=000000><b>$array11[$b]</b></font></td><td align=left><font color=000000><b>$array22[$b]</b></font></td><td align=left><font color=FF0000><b>$final_result</b></font></td></tr>";
												}
						
												$report.= "</table>";
												$report.= "<br>";
												#print "Key name: $array11[$b]\n";
												#print "Value: $array22[$b]\n";
											}
											#print "$table_nam     $pnum+$fnum           $len\n";
						
										}
						
									}
				
								}
				
						}
						
					}
				}
				if(!(($Dimtp[0] eq "DC") and ($Dimtp[2] ne "LLE" and $Dimtp[2] ne "IPRAN" and $Dimtp[2] ne "WLE" and $Dimtp[2] ne "FFAXW" and $Dimtp[2] ne "FFAX")))
				{
				$result.=$result_external;
				$result.=$result_Skip;
				}
				$result.=$result_no;			  
				return $result;

		}
		
	}
############################################################################################################################
if($keysloading eq "true")
   {
     print "********Executing Keys Checking Script********\n";

	 
	 my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> VERIFY KEY LOADING </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my $result1=keysloading();
	    	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($num1) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($num)/ <a href=\"#t3\"><font size = 2 color=magenta><b>No Data in table ($empty_num)/ Skipped ($Skip_Num)</td>";
	 print "Keys_Check:PASS- $num1 FAIL- $num No_Data- $empty_num\n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>$result1\t</td>";
	 $report.= "</table>";
	 
     $report.= getHtmlTail();
     my $file = writeHtml("VERIFY_KEY_LOADING_".$datenew,$report);
	 #print "PARTIAL FILE: $file\n"; 
     $keysloading ="false";
	 print "********Keys Checking Script Test Case is done********\n";
   }