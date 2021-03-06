# -*- coding: utf-8 -*-
from south.utils import datetime_utils as datetime
from south.db import db
from south.v2 import SchemaMigration
from django.db import models


class Migration(SchemaMigration):

    def forwards(self, orm):
        # Adding model 'SeqRun'
        db.create_table(u'seqstats_seqrun', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('run_name', self.gf('django.db.models.fields.CharField')(max_length=200)),
            ('machine_name', self.gf('django.db.models.fields.CharField')(max_length=200)),
            ('date', self.gf('django.db.models.fields.DateField')(null=True, blank=True)),
            ('num_clusters', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('num_clusters_pf', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('percent_pf_clusters', self.gf('django.db.models.fields.FloatField')(null=True, blank=True)),
            ('cluster_density', self.gf('django.db.models.fields.FloatField')(null=True, blank=True)),
            ('cluster_density_pf', self.gf('django.db.models.fields.FloatField')(null=True, blank=True)),
            ('num_reads_per_cluster', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('num_tiles', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('num_lanes', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('specs', self.gf('seqstats.customFields.JSONField')(null=True, blank=True)),
        ))
        db.send_create_signal(u'seqstats', ['SeqRun'])

        # Adding model 'Lane'
        db.create_table(u'seqstats_lane', (
            (u'id', self.gf('django.db.models.fields.AutoField')(primary_key=True)),
            ('lane_num', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('run_name', self.gf('django.db.models.fields.CharField')(max_length=200)),
            ('num_clusters', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('num_clusters_pf', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('percent_pf_clusters', self.gf('django.db.models.fields.FloatField')(null=True, blank=True)),
            ('cluster_density', self.gf('django.db.models.fields.FloatField')(null=True, blank=True)),
            ('cluster_density_pf', self.gf('django.db.models.fields.FloatField')(null=True, blank=True)),
            ('perc_q_ge_30', self.gf('django.db.models.fields.FloatField')(null=True, blank=True)),
            ('num_tiles', self.gf('django.db.models.fields.IntegerField')(null=True, blank=True)),
            ('seqrun', self.gf('django.db.models.fields.related.ForeignKey')(to=orm['seqstats.SeqRun'], null=True, blank=True)),
        ))
        db.send_create_signal(u'seqstats', ['Lane'])


    def backwards(self, orm):
        # Deleting model 'SeqRun'
        db.delete_table(u'seqstats_seqrun')

        # Deleting model 'Lane'
        db.delete_table(u'seqstats_lane')


    models = {
        u'seqstats.lane': {
            'Meta': {'object_name': 'Lane'},
            'cluster_density': ('django.db.models.fields.FloatField', [], {'null': 'True', 'blank': 'True'}),
            'cluster_density_pf': ('django.db.models.fields.FloatField', [], {'null': 'True', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'lane_num': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'num_clusters': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'num_clusters_pf': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'num_tiles': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'perc_q_ge_30': ('django.db.models.fields.FloatField', [], {'null': 'True', 'blank': 'True'}),
            'percent_pf_clusters': ('django.db.models.fields.FloatField', [], {'null': 'True', 'blank': 'True'}),
            'run_name': ('django.db.models.fields.CharField', [], {'max_length': '200'}),
            'seqrun': ('django.db.models.fields.related.ForeignKey', [], {'to': u"orm['seqstats.SeqRun']", 'null': 'True', 'blank': 'True'})
        },
        u'seqstats.seqrun': {
            'Meta': {'object_name': 'SeqRun'},
            'cluster_density': ('django.db.models.fields.FloatField', [], {'null': 'True', 'blank': 'True'}),
            'cluster_density_pf': ('django.db.models.fields.FloatField', [], {'null': 'True', 'blank': 'True'}),
            'date': ('django.db.models.fields.DateField', [], {'null': 'True', 'blank': 'True'}),
            u'id': ('django.db.models.fields.AutoField', [], {'primary_key': 'True'}),
            'machine_name': ('django.db.models.fields.CharField', [], {'max_length': '200'}),
            'num_clusters': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'num_clusters_pf': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'num_lanes': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'num_reads_per_cluster': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'num_tiles': ('django.db.models.fields.IntegerField', [], {'null': 'True', 'blank': 'True'}),
            'percent_pf_clusters': ('django.db.models.fields.FloatField', [], {'null': 'True', 'blank': 'True'}),
            'run_name': ('django.db.models.fields.CharField', [], {'max_length': '200'}),
            'specs': ('seqstats.customFields.JSONField', [], {'null': 'True', 'blank': 'True'})
        }
    }

    complete_apps = ['seqstats']