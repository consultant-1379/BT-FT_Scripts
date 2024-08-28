#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $duplicate_check = "true";
my $result;
my $result_no;
#my $report="";
my $num = 0;
my $num1 = 0;
my $empty_num = 0;

my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
open(Pass, "<Passwords.txt");
	my $name = do {local $/; <Pass> };
	my ($DCPass,$DwhrepPass,$EtlrepPass) = split("\n",$name);
	#print "DCPass: $DCPass , DwhrepPass: $DwhrepPass , EtlrepPass: $DwhrepPass";
sub trim
{
    my $str = $_[0];
    $str=~s/^\s+|\s+$//g;
    return $str;
}

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
																							
sub uniq {
			return keys  %{{ map { $_ => 1 } @_ }};
	}

############################################################


sub duplicate_check{
my @tp_name_array;
			
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
my %node_map;
		
my $node_names = join("','",@input_node_array);
#print Dumper(\@input_node_array);	
				
		if ($tp_name=~ m/(DC_\w*)/){
				
			foreach my $tp ($tp_name) {
				
				$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
				$result .= "<tr><td align=left><font color=black><font size=5px><b>TP NAME:</b></font></font></td><td align=left colspan=2><font color=blue><font size=5px><b>$tp</b></font></font></td></tr>";
				$result .= "<tr><td align=left><font color=000000><b>TABLE NAME</b></font></td><td align=left><font color=000000><b>Node Name</b></font></td><td align=left><font color=000000><b>No.of Duplicate Rows</b></font></td><td align=left><font color=000000><b>RESULT</b></font></td></tr>";
				
				$result_no.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
					
				$result_no.= "<tr><td align=left><font color=magenta><b>NO DATA IN TABLE FOR DATE $date_id:</b></font></td></tr>";
				
				my $tp_query = "select distinct TYPENAME from MeasurementType where TYPEID like '%$package%' and TYPENAME not like '%BH'";
				my ($tp_names) = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$tp_query,"ALL");
				
				undef @tp_name_array;
				
					for my $rows ( @$tp_names ) {
						for my $field ( @$rows ) {
								if ( $field !~ m/^dim*/ || $field !~ m/^DIM*/ )
							{
								push @tp_name_array, $field;
							}
						}
					}
				
				foreach ( @tp_name_array ){
				my $flag =0;
				my $tab_name = $_;
				$tab_name =~ s/ //g;
				#print "$tab_name\n";
				
				my $query2 = "select count(*) from ".$tab_name."_RAW where DATE_ID = '$date_id'";

                my ($res2) = executeSQL("dwhdb",2640,"dc",$DCPass,$query2,"ROW");
							
				if (!$res2){
				
				#print "No data in table\n";
				$empty_num++;
					
					$result_no.= "<tr><td align=left><font color=000000><b>$tab_name</b></font></td></tr>";
				}
				else{
				
				##my $query3 = "select count(*) from ".$tab_name."_RAW where ROWSTATUS like '%DUPLICATE%'" ;
				
				my $query3 = "select $Nodenamekey, count(*) from ".$tab_name."_raw where ROWSTATUS like '%Duplicate%' and DATE_ID ='$date_id' and $Nodenamekey in ('$node_names') group by $Nodenamekey" ;
				
				#print "query :$query3\n";
                my ($res3) = executeSQL("dwhdb",2640,"dc",$DCPass,$query3,"ALL");
#------------------------------------------------------------------------------------
			my $node_names_output;
			my $counttt;
			my $node_length = @input_node_array;
			
			foreach my $str (@input_node_array){
				$node_map{$str} =0;
			}
			
			if(defined $res3){
				foreach my $m (@$res3){
					#my $m = @$res[$i];
					#print Dumper($res3);
					$node_map{@$m[0]} = @$m[1];
					#print "--$node_map{@$m[0]}--\n";
					if ($node_map{@$m[0]} gt 0){
						$flag = 1;
						#print "table :$tab_name\n";
					}
				}
			}
			foreach my $s (keys %node_map){
				$node_names_output .= $s.",";
				$counttt .= $node_map{$s}.",";
			}
			#print "node_names_output :$node_names_output\n";
			#print "counttt :$counttt\n";
			chop($node_names_output);
			chop($counttt);
#------------------------------------------------------------------------------------	
			if($flag == 0){
				
				$result .= "<tr><td align=left><font color=009933><b>$tab_name</b></font></td><td align=left><font color=009933><b>$node_names_output</b></font></td><td align=left><font color=009933><b>$counttt</b></font></td><td align=left><font color=009933><b>PASS</b></font></td></tr>";
				$num1++;
				#print "$num1\n";
				}else{
				
					$result .= "<tr><td align=left><font color=FF0000><b>$tab_name</b></font></td><td align=left><font color=FF0000><b>$node_names_output</b></font></td><td align=left><font color=FF0000><b>$counttt</b></font></td><td align=left><font color=FF0000><b>DUPLICATE ROW FOUND</b></font></td></tr>";
					$num++;
					#print "$num\n";
				}
				}
				}
			}
				$result.= "</table>";
				$result.= "<br>";
		}elsif($tp_name=~ m/(INTF_\w*)/){
			print "$line: This is not a PM TP. This testcase is only for PM TP.\n\n\n";
		}else{
			print "$line: This is not a PM TP. This testcase is only for PM TP.\n\n\n";
		}

$result.=$result_no;
return $result;
}

############################################################################################################################
if($duplicate_check eq "true")
   {
	print "********Executing Duplicate Row Checking Script********\n";


	my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> DUPLICATE ROW CHECK </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my $result1=duplicate_check();
	    	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($num1) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($num)/ <a href=\"#t3\"><font size = 2 color=magenta><b>No Data in table ($empty_num)</td>";
	 print "Duplicate_Check:PASS- $num1 FAIL- $num No_Data- $empty_num\n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>$result1\t</td>";
	 $report.= "</table>";
	 
     $report.= getHtmlTail();
     my $file = writeHtml("DUPLICATE_ROW_CHECK_".$datenew,$report);
	 #print "PARTIAL FILE: $file\n"; 
     $duplicate_check ="false";
	 print "********Duplicate Row Checking Script Test Case is done********\n";
   }

