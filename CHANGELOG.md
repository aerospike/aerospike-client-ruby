## Dec 28 2014 (0.1.6)

  Minor features added, minor fixes and improvements.

  * **New Features**:

    * Added `Policy.consistency_level`
    * Added `WritePolicy.commit_level`

  * **Fixes**

    * Fixed setting timeout on connection
    * Fixed exception handling typo for Connection#write 

## Dec 8 2014 (0.1.5)

  Major features added, minor fixes and improvements.

  * **New Features**:

    * Added `Client.scan_node`, `Client.scan_all`
    * Added `Client.query`

  * **Fixes**

    * Fixed getting back results only for specified bin names.

## Oct 27 2014 (0.1.3)

  Minor fix.

  * **Changes**:

    * Fixed LDT bin and module name packing.

## Oct 25 2014 (0.1.2)

  Minor fix.

  * **Changes**:

    * Fixed String unpacking for single byte strings.

## Oct 25 2014 (0.1.1)

  Minor fixes.

  * **Changes**:

    * Fixed String packing header in Hash and Array.
    * #find on LDTs returns `nil` instad of raising an exception if the item is not found.

## Oct 14 2014 (0.1.0)

  * Initial Release.
