---
title: "PuppetDB 3.0 » Using PuppetDB"
layout: default
canonical: "/puppetdb/latest/using.html"
---

[exported]: /puppet/latest/reference/lang_exported.html


Currently, the main use for PuppetDB is to enable advanced features in Puppet. We expect additional applications to be built on PuppetDB as it becomes more widespread.

If you wish to build applications on PuppetDB, see the navigation sidebar for links to the API spec.

Checking Node Status
-----

The PuppetDB plugins [installed on your puppet master(s)](./connect_puppet_master.html) include a `status` action for the `node` face. On your puppet master, run:

    $ sudo puppet node status <node>

where `<node>` is the name of the node you wish to investigate. This will tell you whether the node is active, when its last catalog was submitted, and when its last facts were submitted.

Using Exported Resources
-----

PuppetDB lets you use exported resources, which allows your nodes to publish information for use by other nodes.

[See here for more about using exported resources.][exported]

