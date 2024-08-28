use strict;
use warnings;

my $file = 'data.txt';
open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
my @array = <FILE>;
my $name = shift @array;
chomp $name;

my $final_name = $name."|";

print "Data loaded for DateTimeID: ";
my $datetime = <STDIN>;
chomp $datetime;
$datetime=~ s/^\s+|\s+$//g;
print "Data loaded for DateTimeID: '$datetime'\n";

$final_name = $final_name.$datetime."|";

print "Node Name Key, example: NE_NAME, is: ";
my $Node_name_key = <STDIN>;
chomp $Node_name_key;
$Node_name_key=~ s/^\s+|\s+$//g;
print "Node Name Key is: '$Node_name_key'\n";

$final_name = $final_name.$Node_name_key.":";

print "Node Name: ";
my $node_name = <STDIN>;
chomp $node_name;
$node_name=~ s/^\s+|\s+$//g;
print "Node Name is: '$node_name'\n";

$final_name = $final_name.$node_name."|";

print "Topology Node Name Key, example: NE_NAME, is: ";
my $TopoKey = <STDIN>;
chomp $TopoKey;
$TopoKey=~ s/^\s+|\s+$//g;
print "Topo Node Name Key: '$TopoKey'\n";

$final_name = $final_name.$TopoKey;


my ($date,$J) = $datetime=~ m/(\d+\-\d+\-\d+)( \d+\:\d+\:\d+)/;
 
open(TIME, ">time.txt");
print TIME $date;
close(TIME);

open(FILE, ">data.txt");
print FILE $name;
close(FILE);

open(FILE, ">data_new.txt");
print FILE $final_name;
close(FILE);


my $Out = `bash /eniq/sw/installer/getPassword.bsh -u dc`;
my ($U,$PASS) = $Out =~ m/(.*\: )(.*)/;
open(Pass, ">Passwords.txt");
print Pass $PASS."\n";
$Out = `bash /eniq/sw/installer/getPassword.bsh -u dwhrep`;
($U,$PASS) = $Out =~ m/(.*\: )(.*)/;
print Pass $PASS."\n";
$Out = `bash /eniq/sw/installer/getPassword.bsh -u etlrep`;
($U,$PASS) = $Out =~ m/(.*\: )(.*)/;
print Pass $PASS."\n";
close(Pass);