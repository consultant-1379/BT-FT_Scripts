#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $countersloading = "true";
my $result;
my $fail_result = 0;
my $pass_result = 0;
my $result_no;
my $empty_num = 0;
my $deprecate_count = 0;
my $deprecate_count_FJ=0;
my $deprecate_count_desc=0;


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
																							
sub uniq {
			return keys  %{{ map { $_ => 1 } @_ }};
	}
##################################################################################################################################
sub countersloading {

my %hash_for_mapping_table_counter = ();
my %hash_for_mapping_table_counter2 = ();
my $final_table_name = ();

my $file = 'data_new.txt';
open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
my @array = <FILE>;
	
my $line = ();
$line = shift @array;
chomp $line;	
#print "$line\n";
my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNameKey) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.+)/;
my $package = $tp_name.":((".$build."))";
#print "package :$package\n";

my ($year,$hyphen1,$month,$hyphen2,$date,$space,$hour,$colon1,$min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;
#my $erbsdatetime = "$year-$month-$date ".(int($hour)+2).":$min:$sec";
my $date_id = "$year-$month-$date";

#EQEV-126991 : BT-FT script to be enhanced to handle multiple Node name.
my @input_node_array= split(",",$Nodename);
		
my $node_names = join("','",@input_node_array);
#print Dumper(\@input_node_array);

if ($tp_name =~ m/(DC_\w*)/) {

	$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr>";
								
	$result_no.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
	$result_no.= "<tr><td align=left><font size=3px><font color=magenta><b>NO DATA IN TABLE FOR DATE $datetime AND NODE-NAME: $Nodename</b></font></font></td></tr>";

	my $query_from_repdb = "select distinct substr(substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3),1,charindex(':',substr(MeasurementColumn.MTABLEID,charindex('))',MeasurementColumn.MTABLEID)+3))-1) as tab, DATANAME as counter from MeasurementColumn where  MTABLEID like '".$package."%:RAW' and COLTYPE like 'COUNTER' order by tab;";
	my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_repdb,"ALL");

	for my $data_from_repdb (@$result_from_repdb) {

		my ($table_name, $counter_name) = @$data_from_repdb;
		# EQEV-117979
		my $new_counter = "count(".$counter_name.")"; 
		push @{$hash_for_mapping_table_counter{$table_name}}, $new_counter;
	}
	
	while ( my ( $key_hash_for_mapping_table_counter , $value_hash_for_mapping_table_counter) = each (%hash_for_mapping_table_counter)){
                                                
		my @uniq_value_hash_for_mapping_table_counter;
        my @temp_array_hash_for_mapping_table_counter = @$value_hash_for_mapping_table_counter;
        my %temp_hash_for_mapping_table_counter = map { $_, 0 } @temp_array_hash_for_mapping_table_counter;
        @uniq_value_hash_for_mapping_table_counter = keys %temp_hash_for_mapping_table_counter;
        push @{$hash_for_mapping_table_counter2{$key_hash_for_mapping_table_counter}} , @uniq_value_hash_for_mapping_table_counter;
    
	}
	
	foreach my $key_for_table ( keys %hash_for_mapping_table_counter ) {
	
		undef $final_table_name;
		$final_table_name = $key_for_table."_RAW";
		chomp($final_table_name);
		
		#$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$final_table_name</b></font></font></td></tr>";
		#$result .= "<tr><td align=left><font color=800000><font size=2px><b>KEY NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>VALUE</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
		my $query_array_for_counters = join(',', @{$hash_for_mapping_table_counter{$key_for_table}});
		my $query_for_dwhdb = ();
		
		if ($final_table_name =~ m/\w*_V_RAW/) {
			#$query_for_dwhdb = "select ".$query_array_for_counters." from ".$final_table_name." WHERE $Nodenamekey like '$Nodename' and DATE_ID ='$date_id' and DCVECTOR_INDEX = 2;" ;
			
			$query_for_dwhdb = "select ".$query_array_for_counters." from ".$final_table_name." WHERE $Nodenamekey in ('$node_names') and DATE_ID ='$date_id' and DCVECTOR_INDEX = 2;" ;
			#print "V :$query_for_dwhdb\n";
		}else {
			
			#$query_for_dwhdb = "select ".$query_array_for_counters." from ".$final_table_name." WHERE $Nodenamekey like '$Nodename' and DATE_ID ='$date_id';" ;

			$query_for_dwhdb = "select ".$query_array_for_counters." from ".$final_table_name." WHERE $Nodenamekey in ('$node_names') and DATE_ID ='$date_id';" ;
			#print "$query_for_dwhdb\n";
		}
		
		my @result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$query_for_dwhdb,"ROW");
		#print Dumper(\@result_from_dwhdb);
		my @final_result_from_dwhdb = join(';', @result_from_dwhdb);
		
		my $table_dep_flag = 0;
		my $cnt=@result_from_dwhdb; # getting counters length
		
		my $num=0;
		foreach my $a (@result_from_dwhdb){
			
			if ($a == '0'){
				$num++;
			}
		}
		if($cnt == $num){
			#print "Empty table :$final_table_name\n";
			$table_dep_flag = 1;
		}
	
		if ($table_dep_flag == 1) {
			
			my $rep_query = "select followjohn from MeasurementType where typename like '$key_for_table' and TYPEID like '$package:%';" ;
			my ($result_repdb_1)= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$rep_query,"ALL");
			
			#table check if deprecated.
			my $Pattern_flag=0;
			for my $r1 (@$result_repdb_1){
				my $temp1=@$r1[0];
				if($temp1 == "0" or $temp1 eq ''){
					$Pattern_flag=1;
				}else{
					my $rep_query_2 = "select description from MeasurementType where typename like '$key_for_table' and TYPEID like '$package:%';" ;
					my ($result_repdb_2)= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$rep_query_2,"ALL");

					for my $r2 (@$result_repdb_2){
						my $temp=@$r2[0];
						my @desc_patterns   = ("Deprecated:(To be deprecated in n-8 release)","Deprecated since", "Deprecated,since", "Deprecated,Since", "Deprecated Since", "Deprecated. Since", "Deprecated. since", "Deprecated only in", "Deprecated in", "Deprecated: Since", "Deprecated:Since", "Deprecated.", "Deprecated,");
										
						for my $pattern (@desc_patterns) {	
							if($temp =~ m/\w*$pattern\w*/){
								$Pattern_flag=1;
								last;
							}						
						}
					}
				}
			}
			if($Pattern_flag == '1'){
				$empty_num++;
				$result_no.= "<tr><td align=left><font color=000000><b>$final_table_name</b></font></td></tr>";
			}else{
				$fail_result++;
				$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$final_table_name</b></font></font></td></tr>";
				$result .= "<tr><td align=left><font color=800000><font size=2px><b>KEY NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>VALUE</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
				$result .= "<tr><td align=left><font color=red><b>All counters Loaded for datetime:</b></font></td><td align=left><font color=red><b>$datetime</b></font></td><td align=left><font color=red><b>FAIL</b></font></td></tr>";
			}		
			
		}else{
			$result .= "<tr><td align=left><font color=blue><font size=3px><b>TABLE NAME</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$final_table_name</b></font></font></td></tr>";
			$result .= "<tr><td align=left><font color=800000><font size=2px><b>KEY NAME</b></font></font></td><td align=left><font color=800000><font size=2px><b>VALUE</b></font></font></td><td align=left><font color=800000><font size=2px><b>RESULT</b></font></font></td></tr>";
		
			$result .= "<tr><td align=left><font color=006633><b>All counters Loaded for datetime:</b></font></td><td align=left><font color=006633><b>$datetime</b></font></td><td align=left><font color=006633><b>PASS</b></font></td></tr>";
			
			my @output_of_dwhdb = undef;
			push (@output_of_dwhdb ,@final_result_from_dwhdb);
						
			my @array_of_repdb_output = split (",",$query_array_for_counters);
						
			my $count_of_repdb_output = @array_of_repdb_output;
			my $randon_number_1;
			
			for ($randon_number_1=0;$randon_number_1<1;$randon_number_1++) {
			
				my @array_of_dwhdb_output = split (";","@output_of_dwhdb");
						
				my $count_of_dwhdb_output = @array_of_dwhdb_output;
				my $randon_number_2;
				#------
				my $countername;
				for ($randon_number_2=0;$randon_number_2<$count_of_dwhdb_output;$randon_number_2++) {
					#EQEV-111465
					my $flag =0;
					my $final_result;
					my $final_result1;
					my $result_from_repdb_1;
					
					if ("$array_of_dwhdb_output[$randon_number_2]" eq '' or $array_of_dwhdb_output[$randon_number_2] == '0' ) {
						#-----
						(my $j,my $j1,$countername,my $j2) = $array_of_repdb_output[$randon_number_2]=~ m/(count)(\()(\w*)(\))/;
						my $query_from_repdb_1 = "select FOLLOWJOHN from MeasurementCounter where DATANAME like '$countername' and TYPEID like '$package:$key_for_table';";
						#--print "array_of_dwhdb_output[$randon_number_2] :$array_of_dwhdb_output[$randon_number_2]\n";
	                    $result_from_repdb_1= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_repdb_1,"ALL");
						
						my $deprecate_count_FJ=0;
						my $deprecate_count_desc=0;
						for my $r1 (@$result_from_repdb_1){
						    my $temp1=@$r1[0];
							if($temp1 == '0'){
								
								$final_result1 = 'Deprecated';
								$flag=1;
							}else{
								my $query_from_repdb_2 = "select description from MeasurementCounter where DATANAME like '$countername' and TYPEID like '$package:$key_for_table';";
								my $result_from_repdb_2= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_repdb_2,"ALL");
						
								for my $r2 (@$result_from_repdb_2){
									my $temp=@$r2[0];
									my $Pattern_flag=0;
									my @desc_patterns   = ("Deprecated:(To be deprecated in n-8 release)","Deprecated since", "Deprecated,since", "Deprecated,Since", "Deprecated Since", "Deprecated. Since", "Deprecated. since", "Deprecated only in", "Deprecated in", "Deprecated: Since", "Deprecated:Since", "Deprecated.", "Deprecated,");
				
									for my $pattern (@desc_patterns) {
											
										if($temp =~ m/\w*$pattern\w*/){
											$Pattern_flag=1;
											last;
										}
									}
									if($Pattern_flag == 1){
										$final_result1 = 'Deprecated';
										$flag=1;											
									}
								}
							}
						}
						if($flag == 0){
							$final_result = 'FAIL';
						}else{
							$final_result = 'FAIL';
					    } 
					}else {
						$final_result = 'PASS';	
					}
					if ($final_result eq 'PASS') {
						$pass_result++;
					}else {
					    if($flag == 0){
							$fail_result++;
							$result .= "<tr><td align=left><font color=FF0000><b>$countername</b></font></td><td align=left><font color=FF0000><b>$array_of_dwhdb_output[$randon_number_2]</b></font></td><td align=left><font color=FF0000><b>$final_result</b></font></td></tr>";
						}else{
							$deprecate_count++;
							$result .= "<tr><td align=left><font color=DarkOrange><b>$countername</b></font></td><td align=left><font color=DarkOrange><b>$array_of_dwhdb_output[$randon_number_2]</b></font></td><td align=left><font color=DarkOrange><b>$final_result1</b></font></td></tr>";
						}
					}
				}
			}
		}	
	}#---
	$result.=$result_no;
	return $result;
	}else {

		print "$tp_name: This is not a PM TP. This testcase is only for PM TP.\n\n\n";
	}


}
############################################################################################################################
if($countersloading eq "true"){
     print "********Executing Counter Checking Script********\n";

	 
	my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> VERIFY COUNTER LOADING </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my $result1=countersloading();
	    	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($pass_result) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($fail_result)/ <a href=\"#t3\"><font size = 2 color=magenta><b>No Data in table ($empty_num) / <a href=\"#t4\"><font size = 2 color=DarkOrange><b>Deprecated ($deprecate_count)</td>";
	 print "Counter_Check:PASS- $pass_result FAIL- $fail_result No_Data- $empty_num\n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>$result1\t</td>";
	 $report.= "</table>";
	 
     $report.= getHtmlTail();
     my $file = writeHtml("VERIFY_COUNTER_LOADING_".$datenew,$report);
	 #print "PARTIAL FILE: $file\n"; 
     $countersloading ="false";
	 
	 
	 print "********Counter Checking Script Test Case is done********\n";
}
############################################################################################################################