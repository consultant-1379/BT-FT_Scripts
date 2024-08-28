#!/usr/bin/perl

#use strict;
#use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;
use XML::Simple;
use File::Basename;


my $verifying_manmods = "true";
my $result;
my $num = 0;
my $num1 = 0;
my %TableTag={};
my $temp1 = "xn:attributes";
my $temp2 = "xn:vsDataType";
my $temp = "xn:VsDataContainer";
my @string = ();

open(Pass, "<Passwords.txt");
	my $name = do {local $/; <Pass> };
	my ($DCPass,$DwhrepPass,$EtlrepPass) = split("\n",$name);
	#print "DCPass: $DCPass , DwhrepPass: $DwhrepPass , EtlrepPass: $DwhrepPass";
my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";

##################################################################################
#             The DATETIME value for the FILENAME of the HTML LOGS               #
##################################################################################

my ($sec,$min,$modification_dataour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
  $mon++;
  $year=1900+$year;
my $datenew =sprintf("%4d%02d%02d%02d%02d%02d",$year,$mon,$mday,$modification_dataour,$min,$wday);


############################################################
# GET TIMESTAMP
# This is a utility 
sub getTime{
  my ($sec,$min,$modification_dataour,$mday,$mon,$year,$wday, $yday,$isdst)=localtime(time);
  return sprintf "%4d-%02d-%02d %02d:%02d:%02d\n", $year+1900,$mon+1,$mday,$modification_dataour,$min,$sec;
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
        $dbh->disconnect;
}

##################################################################################################################################
sub ValidateMo
{
	my $tag = $_[0];
	my $Moid = $_[1];
	my $tagid = $_[2];
	#print "$tagid     ";
	
	# if($tag = '2614' or $tag = '556' or $tag = '1053' or $tag = '414' or $tag = '2282' or $tag = '2735')
	# {
		# print "Just for Reference : $tag      $Moid\n";
	# }
	$Table_name = $TableTag{$tag."_V"};
	#print "$Table_name\n";
	if($TableTag{$tag} ne ''){
	$Table_name = $TableTag{$tag};}
	if($Table_name ne ''){
	$query = "select distinct moid from $Table_name"."_raw where datetime_id = '$datetime' and $Nodenamekey like '$Nodename'";
	$DBT1 = executeSQL("dwhdb",2640,"dc",$DCPass,$query,"ALL");
	my $len = 0;
	for my $i(@$DBT1)
	{ #print "Hii ";
		if(@$i[0] eq $Moid)
		{
			$len = 1;
			last;
		}
	}
	if($len ne 0)
	{
		#print "Pass $tag    $Moid\n";
		$num++;
		#$result .= "<tr><td align=left><font color=Green><font size=2px><b>$DBT[0]</b></font></font></td><td align=left colspan=7><font color=Red><font size=2px><b>$Moid</font></font></td><td align=left colspan=7><font color=Green><font size=2px><b>PASS</font></font></td></tr>";
	}
	else
	{
		#print "Fail: $tag    $Moid\n";
		#print "$query\n";
		$num1++;
		$result .= "<tr><td align=left width=90px><font color=Red><font size=2px><b>$Table_name</b></font></font></td><td align=left colspan=7><font color=Red><font size=2px><b>$Moid</font></font></td></tr>";
	}
	}
	else
	{
		#print "Table Miss : $tag    $Moid\n";
	}
}

sub  Bulkcm
{
	my $Datac;
	my $Reference=0;
	my $file = 'data_new.txt';
	open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
	my @array = <FILE>;
	my $line = shift @array;
	chomp $line;	
	#print "$line\n";
	
	
	($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$Toponodename) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.*)/;
	$package = $tp_name.":((".$build."))";
	@array = split(",",$Nodename);
	$Nodename = $array[0];
	print "$package\n";

	$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr>";
	$result .= "<tr><td align=left><font color=black><font size=2px><b>Table Name</b></font></font></td><td align=left colspan=7><font color=black><font size=2px><b>MOID from the File</font></font></td></tr>";
	
	$query = "select substr(substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3),1,charindex(':',substr(defaulttags.DATAFORMATID,charindex('))',defaulttags.DATAFORMATID)+3))-1),tagid from defaulttags where dataformatid like '%$package%' order by tagid";
	$DBT = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query,"ALL");
	for my $arr(@$DBT)
	{
		$TableTag{@$arr[1]} = @$arr[0];
	}
	# open my $FH, '>', 'Tags.txt';
	# print $FH Dumper(\%TableTag);
	# close $FH;
	print "Please Enter Bulk_Cm file name:\n";
	my $file_name = <>; #"AOM901076_R1A_R6.0_202304190858+0000_eniq_oss_1_ERBSG201.xml";
	$file_name =~ s/^\s+|\s+$//g;
	chomp($file_name);
	print "Entered file name is '",basename($file_name),"'\n";	
	#changing the file format Dos to Unix
	system("dos2unix $file_name");

	my $file1 = basename($file_name);
	# create object
	my $xml = new XML::Simple;
	
	# read XML file
	my $data = $xml->XMLin("$file_name");
	#print Dumper(\@data);
	# open my $FH, '>', 'outfile.txt';
	# print $FH Dumper(\@data);
	# close $FH;
	my $Bow = $data->{configData};
	my $t="xn:SubNetwork";
	if(grep(/^xn:SubNetwork$/,keys %$Bow))
	{
		my $subnet1 = $Bow->{$t}->{id};
		#print Dumper(\$subnet1);
		$Bow = $Bow->{$t};
		print "Sub1:$subnet1\n";
	}
	if(grep(/^xn:SubNetwork$/,keys %$Bow))
	{
		my $subnet2 = $Bow->{$t}->{id};
		print "Sub2:$subnet2\n";
		$Bow = $Bow->{$t};
	}
	
	if(grep(/^xn:MeContext$/,keys %$Bow))
	{
		my $te = "xn:MeContext";
		$mecontext = $Bow->{$te}->{id};
		$Mo = "MeContext=$mecontext";
		print "Mecontext:$mecontext\n";
		$Bow = $Bow->{$te};
				
		if(grep(/^xn:VsDataContainer$/,keys %$Bow))
		{
			$reference = 1;
			$Datac = $Bow->{$temp}->{id};
			if($Datac ne '')
			{
				#print "Datac:$Datac\n";
				my ($vsadata,$tagid) = split("vsData",$Bow->{$temp}->{$temp1}->{$temp2});
				if($tagid ne '')
				{
					ValidateMo($tagid,"$tagid=$Datac",$tagid);
				}
				$len = keys %{$Bow->{$temp}->{$temp}};
				if($len ge 0){$Me_Bow = $Bow->{$temp}->{$temp};DataCon($Me_Bow);}
			}
			else
			{
				$len = keys %{$Bow->{$temp}};
				if($len ge 0){$Me_Bow = $Bow->{$temp};DataCon($Me_Bow);}
			}
		}
	}
	if(grep(/^xn:ManagedElement$/,keys %$Bow))
	{
		my $tem = "xn:ManagedElement";
		$ManagedElement = $Bow->{$tem}->{id};
		$Mo="ManagedElement=$ManagedElement";
		if(grep(/^xn:VsDataContainer$/,keys %{$Bow->{$tem}}))
		{
			$reference = 1;
			$Datac = $Bow->{$tem}->{$temp}->{id};
			if($Datac ne '')
			{
				#print "Manage Datac: $Datac\n";
				my ($vsadata,$tagid) = split("vsData",$ManagedElement->{$temp}->{$temp1}->{$temp2});
				if($tagid ne '')
				{
					ValidateMo($tagid,"$tagid=$Datac",$tagid);
				}
				$len = keys %{$Bow->{$tem}->{$temp}->{$temp}};
				if($len ge 0){$Ma_Bow = $Bow->{$tem}->{$temp}->{$temp};DataCon($Ma_Bow);}
			}
			else
			{
				$len = keys %{$Bow->{$tem}->{$temp}};
				if($len ge 0){$Ma_Bow = $Bow->{$tem}->{$temp};DataCon($Ma_Bow);}
			}
		}
	}
	if($num1 eq 0)
	{
		$result .= "<tr><td align=left width=90px><font color=Green><font size=2px><b>All Moids from the file are loaded into respective tables </b></font></font></td><td align=left colspan=7><font color=Green><font size=2px><b> </font></font></td></tr>";
	}
	$result.="</table>";
	return $result;
}

sub DataCon
{
	my $Throw = $_[0];
	#print "$Throw";
	#print Dumper(\keys %$Throw);
	for $key(keys %$Throw)
	{
		@string = ();
		my ($vsadata,$tagid) = split("vsData",$Throw->{$key}->{$temp1}->{$temp2});
		push(@string,$Mo,"$tagid=$key");
		ValidateMo($tagid,join(",",@string),$tagid);
		if(grep(/^xn:VsDataContainer$/,keys %$Throw->{$key}))
		{
			Vsdatacon($Throw->{$key}->{$temp});
		}
	}
}

sub Vsdatacon
{
	my $key = $_[0];
	#print "DataconDatac:$key->{id}\n";
	my ($vsadata,$tagid) = split("vsData",$key->{$temp1}->{$temp2});
	#print "DataconTagid:$tagid\n";
	push(@string,"$tagid=$key->{id}");
	ValidateMo($tagid,join(",",@string),$tagid);
	if(grep(/^xn:VsDataContainer$/,keys %$key))
	{
		Vsdatacon($key->{$temp});
	}
}



print "********Executing BulkCm_MOID_Validation Script********\n";
	$num=0;
	$num1=0;
	my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> BulkCm_MOID_Validation </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	$report.= "<td><b>$stime\t</td>";
	 
	my $result1=Bulkcm();
	#print "$num\n$num1\n";
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>END TIME:\t</td>";
    my $etime = getTime();
	$report.= "<td><b>$etime\t</td>";
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	$report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($num) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($num1)</td>";
	print "BulkCm_MOID_Validation:PASS- $num FAIL- $num1\n";
	$report.= "</table>";
	$report.= "<br>";
	 
	#$result.="<h2> $result1 </h2>";
	$report.="$result1";
	 
    $report.= getHtmlTail();
    my $file = writeHtml("BulkCm_MOID_Validation".$datenew,$report);
	#print "PARTIAL FILE: $file\n"; 
    #$dataloadingmo ="false";
print "********BulkCm_MOID_Validation Test Case is done********\n";
	
